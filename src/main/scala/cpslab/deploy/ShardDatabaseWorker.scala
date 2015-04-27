package cpslab.deploy

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVectorWrapper

private[deploy] class ShardDatabaseWorker(conf: Config, lshInstance: LSH) extends Actor {

  private val maxShardNumPerTable = conf.getInt("cpslab.lsh.sharding.maxShardNumPerTable")
  private val shardingNamespace = conf.getString("cpslab.lsh.sharding.namespace")
  private val shardingExtension = ClusterSharding(context.system)
  private val regionActor = shardingExtension.shardRegion(
    ShardDatabaseWorker.shardDatabaseWorkerActorName)
  
  private lazy val maxDatabaseNodeNum = conf.getInt("cpslab.lsh.sharding.maxDatabaseNodeNum")
  private lazy val shardDatabase = new Array[ActorRef](maxDatabaseNodeNum)

  // data structures for message batching
  private val loadBatchingDuration = conf.getLong("cpslab.lsh.sharding.loadBatchingDuration")
  private lazy val perTableAllocationWriteBuffer = new mutable.HashMap[Int,
    mutable.HashMap[ShardId, List[SparseVectorWrapper]]]
  private lazy val flatAllocationWriteBuffer = new mutable.HashMap[ShardId,
    mutable.HashMap[Int, List[SparseVectorWrapper]]]
  private var batchSender: Cancellable = null

  override def preStart(): Unit = {
    // initialize the sender for load batching
    if (loadBatchingDuration > 0) {
      val system = context.system
      import system.dispatcher
      batchSender = context.system.scheduler.schedule(
        0 milliseconds, loadBatchingDuration milliseconds, new Runnable {
          override def run(): Unit = {
            sendShardAllocation()
          }
        })
    }
  }

  override def postStop(): Unit = {
    if (batchSender != null) {
      batchSender.cancel()
    }
  }

  /**
   * processing logic for search request
   * @param searchRequest the search requeste received from client
   */
  private def processSearchRequest(searchRequest: SearchRequest): Unit = {
    val indexInAllTables = lshInstance.calculateIndex(searchRequest.vector)
    val outputShardMap = new mutable.HashMap[ShardId,
      mutable.HashMap[Int, List[SparseVectorWrapper]]]
    for (tableId <- 0 until indexInAllTables.length) {
      val indexInTable = new Array[Int](indexInAllTables.length)
      indexInTable(tableId) = indexInAllTables(tableId)
      val bucketIndex = indexInAllTables(tableId)
      val vector = List(SparseVectorWrapper(indexInTable, searchRequest.vector))
      shardingNamespace match {
        case "independent" =>
          outputShardMap.getOrElseUpdate(tableId.toString,
            new mutable.HashMap[Int, List[SparseVectorWrapper]]) += bucketIndex -> vector
        case "flat" =>
          val shardId = bucketIndex % maxShardNumPerTable
          outputShardMap.getOrElseUpdate(shardId.toString,
            new mutable.HashMap[Int, List[SparseVectorWrapper]]) += tableId -> vector
      }
    }
    sendOrBatchShardAllocation(outputShardMap)
  }

  private def sendOrBatchShardAllocation(
      outputShardMap: mutable.HashMap[ShardId, mutable.HashMap[Int, List[SparseVectorWrapper]]]
      ): Unit = {
    for ((shardId, tableMap) <- outputShardMap) {
      shardingNamespace match {
        case "independent" =>
          for ((tableId, vectors) <- tableMap) {
            if (loadBatchingDuration <= 0) {
              val shardMap = new mutable.HashMap[Int,
                mutable.HashMap[ShardId, List[SparseVectorWrapper]]]
              shardMap.getOrElseUpdate(tableId,
                new mutable.HashMap[ShardId, List[SparseVectorWrapper]]) += shardId -> vectors
              regionActor ! PerTableShardAllocation(shardMap)
            } else {
              perTableAllocationWriteBuffer.synchronized {
                val vectorsInBatching = perTableAllocationWriteBuffer.getOrElseUpdate(tableId,
                  new mutable.HashMap[ShardId, List[SparseVectorWrapper]]).
                  getOrElseUpdate(shardId, List[SparseVectorWrapper]())
                perTableAllocationWriteBuffer(tableId)(shardId) = vectorsInBatching ++ vectors
              }
            }
          }
        case "flat" =>
          if (loadBatchingDuration <= 0) {
            val map = new mutable.HashMap[ShardId, mutable.HashMap[Int, List[SparseVectorWrapper]]]
            map.getOrElseUpdate(shardId, tableMap)
            regionActor ! FlatShardAllocation(map)
          } else {
            flatAllocationWriteBuffer.synchronized {
              val tableId = tableMap.keysIterator.toList.head
              val vectors = tableMap.values.head
              val vectorsInBatching = flatAllocationWriteBuffer.
                getOrElseUpdate(shardId, new mutable.HashMap[Int, List[SparseVectorWrapper]]).
                getOrElseUpdate(tableId, List[SparseVectorWrapper]())
              flatAllocationWriteBuffer(shardId)(tableId) = vectorsInBatching ++ vectors
            }
          }
      }
    }
  }

  private def processShardAllocation(shardAllocation: ShardAllocation) {
    val shardMap = shardAllocation match {
      case perTableAllocation @ PerTableShardAllocation(_) =>
        perTableAllocation.shardsMap.map{
          case (tableId, bucketAllocation) => {
            val transformedAlloc = bucketAllocation.map{
              case (bucketIndex, vectors) => (bucketIndex.toInt, vectors)}
            (tableId, transformedAlloc)
          }
        }
      case flatAllocation @ FlatShardAllocation(_) =>
        flatAllocation.shardsMap
    }
    for ((_, shardAllocationPerUnit) <- shardMap ;
         (allocationId, vectors) <- shardAllocationPerUnit) {
      val storageNodeIndex = allocationId % maxDatabaseNodeNum
      if (shardDatabase(storageNodeIndex) == null) {
        val newActor = context.actorOf(ShardDatabaseStorage.props(conf),
          name = s"StorageNode-$storageNodeIndex")
        shardDatabase(storageNodeIndex) = newActor
      }
      val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
      indexMap += allocationId -> vectors
      shardDatabase(storageNodeIndex).tell(LSHTableIndexRequest(indexMap), sender())
    }
  }

  private def sendShardAllocation(): Unit = {
    shardingNamespace match {
      case "independent" =>
        perTableAllocationWriteBuffer.synchronized {
          for ((tableId, perTableMap) <- perTableAllocationWriteBuffer) {
            val sendMap = new mutable.HashMap[Int,
              mutable.HashMap[ShardId, List[SparseVectorWrapper]]]
            sendMap += tableId -> perTableMap
            regionActor ! PerTableShardAllocation(sendMap)
          }
          perTableAllocationWriteBuffer.clear()
        }
      case "flat" =>
        flatAllocationWriteBuffer.synchronized {
          for ((shardId, perShardMap) <- flatAllocationWriteBuffer) {
            val sendMap = new mutable.HashMap[ShardId,
              mutable.HashMap[Int, List[SparseVectorWrapper]]]
            sendMap += shardId -> perShardMap
            regionActor ! FlatShardAllocation(sendMap)
          }
          flatAllocationWriteBuffer.clear()
        }
    }
  }

  override def receive: Receive = {
    case searchRequest @ SearchRequest(_) =>
      processSearchRequest(searchRequest)
    case shardAllocation: ShardAllocation =>
      processShardAllocation(shardAllocation)
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

