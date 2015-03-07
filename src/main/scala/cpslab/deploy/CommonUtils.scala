package cpslab.deploy

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.{Config, ConfigFactory}

object CommonUtils {

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          conf: Config): (Config, ActorSystem) = {
    val system = ActorSystem("LSH", conf)
    val maxShardNum = conf.getInt("cpslab.allpair.maxShardNum")
    val maxEntryNum = conf.getInt("cpslab.allpair.maxEntryNum")
    // fix the entry Id to send to a proxy and then spawn to the multiple indexWorkers,
    // otherwise, it is impossible to send data packet to multiple entries just
    // through idExtractor
    val entryIdExtractor: ShardRegion.IdExtractor = {
      case x => ("", x)
    }
    val shardIdResolver: ShardRegion.ShardResolver = msg => msg match {
      case x => ""
    }
    ClusterSharding(system).start(
      typeName = ShardDatabaseWorker.shardDatabaseWorkerActorName,
      entryProps = entryProps,
      idExtractor = entryIdExtractor,
      shardResolver = shardIdResolver
    )
    (conf, system)
  }

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          akkaConfPath: String,
                                          appConfPath: String): (Config, ActorSystem) = {

    val conf = ConfigFactory.parseFile(new File(akkaConfPath)).
      withFallback(ConfigFactory.parseFile(new File(appConfPath))).
      withFallback(ConfigFactory.load())

    startShardingSystem(entryProps, conf)
  }
}

