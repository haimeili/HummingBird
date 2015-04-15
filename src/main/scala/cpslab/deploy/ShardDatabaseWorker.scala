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
      val batchSender = context.system.scheduler.schedule(
        0 milliseconds, loadBatchingDuration milliseconds, self, IOTicket)
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
    for (i <- 0 until indexInAllTables.length) {
      val indexInTable = new Array[Int](indexInAllTables.length)
      indexInTable(i) = indexInAllTables(i)
      val bucketIndex = indexInAllTables(i) % maxShardNumPerTable
      val vectorInList =
        List(SparseVectorWrapper(indexInTable, searchRequest.vector))
      shardingNamespace match {
        case "independent" =>
          outputShardMap.getOrElseUpdate(i.toString,
            new mutable.HashMap[Int, List[SparseVectorWrapper]]) += bucketIndex -> vectorInList
        case "flat" =>
          outputShardMap.getOrElseUpdate(bucketIndex.toString,
            new mutable.HashMap[Int, List[SparseVectorWrapper]]) += i -> vectorInList
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
              val vectorsInBatching = perTableAllocationWriteBuffer.getOrElseUpdate(tableId,
                new mutable.HashMap[ShardId, List[SparseVectorWrapper]]).
                getOrElseUpdate(shardId, List[SparseVectorWrapper]())
              perTableAllocationWriteBuffer(tableId)(shardId) = vectorsInBatching ++ vectors
            }
          }
        case "flat" =>
          if (loadBatchingDuration <= 0) {
            val map = new mutable.HashMap[ShardId, mutable.HashMap[Int, List[SparseVectorWrapper]]]
            map.getOrElseUpdate(shardId, tableMap)
            regionActor ! FlatShardAllocation(map)
          } else {
            val tableId = tableMap.keysIterator.toList.head
            val vectors = tableMap.values.head
            val vectorsInBatching = flatAllocationWriteBuffer.getOrElseUpdate(shardId,
              new mutable.HashMap[Int, List[SparseVectorWrapper]]).
              getOrElseUpdate(tableId, List[SparseVectorWrapper]())
            flatAllocationWriteBuffer(shardId)(tableId) = vectorsInBatching ++ vectors
          }
      }
    }
  }

  private def processShardAllocation(shardAllocation: ShardAllocation) {
    val shardMap = shardAllocation match {
      case perTableAllocation @ PerTableShardAllocation(_) =>
        perTableAllocation.shardsMap
      case flatAllocation @ FlatShardAllocation(_) =>
        flatAllocation.shardsMap.map{case (shardId, perTableAllocation) => {
          val transformedAlloc = perTableAllocation.map{
            case (tableId, vectors) => (tableId.toString, vectors)}
          (shardId, transformedAlloc)
        }}
    }
    for ((_, shardAllocationPerUnit) <- shardMap ;
         (allocationIdStr, vectors) <- shardAllocationPerUnit) {
      val id = allocationIdStr.toInt
      val storageNodeIndex = id % maxDatabaseNodeNum
      if (shardDatabase(storageNodeIndex) == null) {
        val newActor = context.actorOf(ShardDatabaseStorage.props(conf),
          name = s"StorageNode-$storageNodeIndex")
        shardDatabase(storageNodeIndex) = newActor
      }
      val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
      indexMap += id -> vectors
      shardDatabase(storageNodeIndex).tell(LSHTableIndexRequest(indexMap), sender())
    }
  }

  private def sendShardAllocation(): Unit = {
    shardingNamespace match {
      case "independent" =>
        for ((tableId, perTableMap) <- perTableAllocationWriteBuffer) {
          val sendMap = new mutable.HashMap[Int,
            mutable.HashMap[ShardId, List[SparseVectorWrapper]]]
          sendMap += tableId -> perTableMap
          regionActor ! PerTableShardAllocation(sendMap)
        }
        perTableAllocationWriteBuffer.clear()
      case "flat" =>
        for ((shardId, perShardMap) <- flatAllocationWriteBuffer) {
          val sendMap = new mutable.HashMap[ShardId,
            mutable.HashMap[Int, List[SparseVectorWrapper]]]
          sendMap += shardId -> perShardMap
          regionActor ! FlatShardAllocation(sendMap)
        }
        flatAllocationWriteBuffer.clear()
    }
  }

  override def receive: Receive = {
    case searchRequest @ SearchRequest(_) =>
      processSearchRequest(searchRequest)
    case shardAllocation: ShardAllocation =>
      processShardAllocation(shardAllocation)
    case IOTicket =>
      sendShardAllocation()
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

