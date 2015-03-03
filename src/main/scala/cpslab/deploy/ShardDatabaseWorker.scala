package cpslab.deploy

import akka.actor.Actor
import com.typesafe.config.{Config, ConfigException}
import cpslab.lsh.LSH
import cpslab.storage.{CacheEngine, EmptyCacheEngine, KeyValueEngine, LevelDBEngine}

private[deploy] class ShardDatabaseWorker(conf: Config, lshInstance: LSH) extends Actor{

  private var kvEngine: KeyValueEngine = _
  private var cacheEngine: CacheEngine = _
  
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
    null  
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

