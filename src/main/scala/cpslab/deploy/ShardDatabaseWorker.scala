package cpslab.deploy

import akka.actor.Actor
import com.typesafe.config.{ConfigException, Config}
import cpslab.storage.{EmptyCacheEngine, CacheEngine, LevelDBEngine, KeyValueEngine}

private[deploy] class ShardDatabaseWorker(conf: Config) extends Actor{

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
  }
  
  override def receive: Receive = {
    null  
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

