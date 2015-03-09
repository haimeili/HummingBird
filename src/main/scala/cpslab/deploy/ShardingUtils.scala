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

  private val entryIdExtractor: ShardRegion.IdExtractor = {
    case req @ SearchRequest(_, _, _) =>
      (Random.nextInt(maxEntryNum).toString, req)
    case shardAllocation @ ShardAllocation(_) =>
      (Random.nextInt(maxEntryNum).toString, shardAllocation)
    case indexRequest @ LSHTableIndexRequest(_) => 
      //since we ensure that all vectors here belongs to the same entry 
      //we only need to calculate entry id, based on one table data
      ((indexRequest.indexMap.keySet.toList(0) % maxEntryNum).toString, indexRequest)
  }
  
  private val shardIdResolver: ShardRegion.ShardResolver = msg => msg match {
    case searchRequest @ SearchRequest(_, _, _) =>
      //TODO: assign to local shards
      Random.nextInt(maxShardNum).toString 
    case shardAllocation @ ShardAllocation(_) =>
      // since we assume all ids in this message belongs to the same ShardRegion
      // we can simply pick one of the shardIDs as the generated shardID
      shardAllocation.shardsMap(0).keySet.toList(0)
  }
  
  private def initShardAllocation(conf: Config, lsh: LSH): Unit = {
    maxShardNum = conf.getInt("cpslab.lsh.sharding.maxShardNum")
    maxEntryNum = conf.getInt("cpslab.lsh.sharding.maxShardDatabaseWorkerNum")
    lshInstance = lsh
  }

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          conf: Config, lsh: LSH): (Config, ActorSystem) = {
    localShardingSystem = ActorSystem("LSH", conf)
    initShardAllocation(conf, lsh)
    
    require(maxEntryNum > 0 && maxShardNum > 0 & lshInstance != null, 
      "please run ShardingUtils.initShardAllocation before you start Cluster Sharding System")
    
    ClusterSharding(localShardingSystem).start(
      typeName = ShardDatabaseWorker.shardDatabaseWorkerActorName,
      entryProps = entryProps,
      idExtractor = entryIdExtractor,
      shardResolver = shardIdResolver
    )
    (conf, localShardingSystem)
  }

  private[deploy] def startShardingSystem(entryProps: Option[Props],
                                          akkaConfPath: String,
                                          appConfPath: String,
                                          lsh: LSH): (Config, ActorSystem) = {

    val conf = ConfigFactory.parseFile(new File(akkaConfPath)).
      withFallback(ConfigFactory.parseFile(new File(appConfPath))).
      withFallback(ConfigFactory.load())

    startShardingSystem(entryProps, conf, lsh)
  }
}

