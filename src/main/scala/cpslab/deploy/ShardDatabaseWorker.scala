package cpslab.deploy

import java.util

import scala.collection.mutable

import akka.actor.{Actor, ActorRef}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import com.typesafe.config.{Config, ConfigException}
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVectorWrapper
import cpslab.storage.{CacheEngine, EmptyCacheEngine, KeyValueEngine, LevelDBEngine}

private[deploy] class ShardDatabaseWorker(conf: Config, lshInstance: LSH) extends Actor{

  private var kvEngine: KeyValueEngine = _
  private var cacheEngine: CacheEngine = _
  private val maxShardNumPerTable = conf.getInt("cpslab.lsh.sharding.maxShardNumPerTable")
  private val shardingNamespace = conf.getString("cpslab.lsh.sharding.namespace")
  private val shardingExtension = ClusterSharding(context.system)
  private val regionActor = shardingExtension.shardRegion(
    ShardDatabaseWorker.shardDatabaseWorkerActorName)
  
  private lazy val maxDatabaseNodeNum = conf.getInt("cpslab.lsh.sharding.maxDatabaseNodeNum")
  private lazy val shardDatabase = new Array[ActorRef](maxDatabaseNodeNum)

  override def preStart(): Unit = {
    def initKVEngine: KeyValueEngine = {
      //start kvEngine
      try {
        conf.getString("cpslab.lsh.kvEngineName") match {
          case "LevelDB" => new LevelDBEngine
        }
      } catch {
        case e: ConfigException.Missing => new LevelDBEngine
      }
    }
    def initCacheEngine: CacheEngine = {
      //start kvEngine
      try {
        conf.getString("cpslab.lsh.cacheEngineName") match {
          case "LevelDB" => new EmptyCacheEngine
        }
      } catch {
        case e: ConfigException.Missing => new EmptyCacheEngine
      }
    }
    kvEngine = initKVEngine
    cacheEngine = initCacheEngine
  }

  private def processSearchRequest(searchRequest: SearchRequest): Unit = {
    val indexInAllTables = lshInstance.calculateIndex(searchRequest.vector)
    val outputShardMap = new mutable.HashMap[ShardId,
      mutable.HashMap[Int, List[SparseVectorWrapper]]]
    for (i <- 0 until indexInAllTables.length) {
      val indexInTable = new Array[Array[Byte]](indexInAllTables.length)
      val shardID = util.Arrays.hashCode(indexInAllTables(i)) % maxShardNumPerTable
      indexInTable(i) = indexInAllTables(i)
      val vectorInList =
        List(SparseVectorWrapper(searchRequest.vectorId, indexInTable, searchRequest.vector))
      shardingNamespace match {
        case "independent" =>
          outputShardMap.getOrElseUpdate(i.toString,
            new mutable.HashMap[Int, List[SparseVectorWrapper]]) += shardID -> vectorInList
        case "flat" =>
          outputShardMap.getOrElseUpdate(shardID.toString,
            new mutable.HashMap[Int, List[SparseVectorWrapper]]) += i -> vectorInList
      }
    }
    sendShardAllocation(outputShardMap)
  }

  private def sendShardAllocation(
      outputShardMap: mutable.HashMap[ShardId, mutable.HashMap[Int, List[SparseVectorWrapper]]]):
    Unit = {
    for ((shardId, tableMap) <- outputShardMap) {
      shardingNamespace match {
        case "independent" =>
          val shardMap = new mutable.HashMap[Int,
            mutable.HashMap[ShardId, List[SparseVectorWrapper]]]
          for ((tableId, vectors) <- tableMap) {
            shardMap.getOrElseUpdate(tableId,
              new mutable.HashMap[ShardId, List[SparseVectorWrapper]]) += shardId -> vectors
            regionActor ! PerTableShardAllocation(shardMap)
          }
        case "flat" =>
          val map = new mutable.HashMap[ShardId, mutable.HashMap[Int, List[SparseVectorWrapper]]]
          map.getOrElseUpdate(shardId, tableMap)
          regionActor ! FlatShardAllocation(map)
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

  override def receive: Receive = {
    case searchRequest @ SearchRequest(_, _) =>
      processSearchRequest(searchRequest)
    case shardAllocation: ShardAllocation =>
      processShardAllocation(shardAllocation)
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

