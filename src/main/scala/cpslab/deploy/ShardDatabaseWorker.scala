package cpslab.deploy

import java.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{ActorRef, Actor}
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
  
  private def handleSearchRequest(searchRequest: SearchRequest) = shardingNamespace match {
    case "independent" =>
      val indexInAllTables = lshInstance.calculateIndex(searchRequest.vector)
      for (i <- 0 until indexInAllTables.size) {
        val shardMap = new mutable.HashMap[Int, mutable.HashMap[ShardId, List[SparseVectorWrapper]]]
        val indexInTable = new Array[Array[Byte]](indexInAllTables.size)
        val indexInInteger = util.Arrays.hashCode(indexInTable(i)) % maxShardNumPerTable
        indexInTable(i) = indexInAllTables(i)
        val vectorInList =
          List(SparseVectorWrapper(searchRequest.vectorId, indexInTable, searchRequest.vector))
        shardMap.getOrElseUpdate(i, new mutable.HashMap[ShardId, List[SparseVectorWrapper]]) +=
          indexInInteger.toString -> vectorInList
        regionActor ! ShardAllocation(shardMap)  
      }
  } 
  
  private def handleShardAllocation(shardAllocation: ShardAllocation) = shardingNamespace match {
    case "independent" =>
      for ((tableID, shardAllocationPerTable) <- shardAllocation.shardsMap; 
           (shardIDStr, vectors) <- shardAllocationPerTable) {
        val shardID = shardIDStr.toInt
        val storageNode = shardID % maxDatabaseNodeNum
        if (shardDatabase(storageNode) == null) {
          val newActor = context.actorOf(ShardDatabaseStorage.props(conf),
            name = s"StorageNode-$storageNode")
          shardDatabase(storageNode) = newActor
        }
        val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
        indexMap += shardID -> vectors
        shardDatabase(storageNode).tell(LSHTableIndexRequest(indexMap), sender())
      }
  }
  
  override def receive: Receive = {
    case searchRequest @ SearchRequest(_, _) =>
      handleSearchRequest(searchRequest)
    case shardAllocation @ ShardAllocation(_) =>
      handleShardAllocation(shardAllocation)
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

