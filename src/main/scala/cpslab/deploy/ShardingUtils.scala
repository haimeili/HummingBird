package cpslab.deploy

import java.io.File

import scala.util.Random

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.lsh.LSH

private[cpslab] object ShardingUtils {
  
  private var maxShardNum = -1
  private var maxEntryNum = -1
  private var lshInstance: LSH = _
  private var localShardingSystem: ActorSystem = _

  private val flatNamespaceNamespaceEntryResolver: ShardRegion.IdExtractor = {
    case req @ SearchRequest(_) =>
      (Random.nextInt(maxEntryNum).toString, req)
    case shardAllocation @ FlatShardAllocation(_) =>
      (Random.nextInt(maxEntryNum).toString, shardAllocation)
  }

  private val flatNamespaceShardResolver: ShardRegion.ShardResolver = {
    case searchRequest @ SearchRequest(_) =>
      Random.nextInt(maxShardNum).toString
    case shardAllocation @ FlatShardAllocation(_) =>
      shardAllocation.shardsMap.head._1
  }

  private def initShardAllocation(conf: Config, lsh: LSH): Unit = {
    maxShardNum = conf.getInt("cpslab.lsh.sharding.maxShardNumPerTable")
    maxEntryNum = conf.getInt("cpslab.lsh.sharding.maxShardDatabaseWorkerNum")
    lshInstance = lsh
  }

  private[deploy] def startShardingSystem(
      entryProps: Option[Props],
      conf: Config, 
      lsh: LSH): (Config, ActorSystem) = {

    val shardingSystemName = conf.getString("cpslab.lsh.sharding.systemName")
    localShardingSystem = ActorSystem(shardingSystemName, conf)
    initShardAllocation(conf, lsh)
    require(maxEntryNum > 0 && maxShardNum > 0 & lshInstance != null,
      "please run ShardingUtils.initShardAllocation before you start Cluster Sharding System")
    
    // resolve different shard/entry resolver
    val (shardResolver, entryResolver) =
      (flatNamespaceShardResolver, flatNamespaceNamespaceEntryResolver)
    
    ClusterSharding(localShardingSystem).start(
      typeName = ShardDatabaseWorker.shardDatabaseWorkerActorName,
      entryProps = entryProps,
      idExtractor = entryResolver,
      shardResolver = shardResolver
    )
    (conf, localShardingSystem)
  }

  private[deploy] def startShardingSystem(
      entryProps: Option[Props],
      akkaConfPath: String,
      appConfPath: String,
      lsh: LSH): (Config, ActorSystem) = {

    val conf = ConfigFactory.parseFile(new File(akkaConfPath)).
      withFallback(ConfigFactory.parseFile(new File(appConfPath))).
      withFallback(ConfigFactory.load())

    startShardingSystem(entryProps, conf, lsh)
  }
}

