package cpslab.deploy

import java.util

import scala.collection.mutable

import akka.actor.Actor
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import com.typesafe.config.{Config, ConfigException}
import cpslab.lsh.LSH
import cpslab.storage.{CacheEngine, EmptyCacheEngine, KeyValueEngine, LevelDBEngine}

private[deploy] class ShardDatabaseWorker(conf: Config, lshInstance: LSH) extends Actor{

  private var kvEngine: KeyValueEngine = _
  private var cacheEngine: CacheEngine = _
  private val maxShardNum = conf.getInt("cpslab.lsh.sharding.maxShardNum")
  private val shardingExtension = ClusterSharding(context.system)
  private val regionActor = shardingExtension.shardRegion(
    ShardDatabaseWorker.shardDatabaseWorkerActorName)
  
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
  
  override def receive: Receive = {
    case searchRequest @ SearchRequest(vectorId, vector, k) =>
      val indexInAllTables = lshInstance.calculateIndex(searchRequest.vector)
      val shardAllocationTable = new mutable.HashMap[Int, mutable.HashMap[ShardId, String]]
      for (i <- 0 until indexInAllTables.size) {
        val indexInATable = util.Arrays.hashCode(indexInAllTables(i))
        val shardID = indexInATable % maxShardNum
        shardAllocationTable.getOrElseUpdate(i, new mutable.HashMap[ShardId, String]) +=
          shardID.toString -> vectorId
      }
      // TODO: add write buffer and merge request sent to the same node
      // TODO: we need to carefully ensure the correctness of the merge request, when there is no 
      // such shard in the cache, we can only separate into different messages
      //shardingExtension.
    case shardAllocation @ ShardAllocation(_) => 
      // TODO calculate the LSHTableIndexRequest ensuring that all vectors in an
      // LSHTableIndexRequest belongs to the same entry
    case indexRequest @ LSHTableIndexRequest(_) => 
      // TODO: index the elements
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

