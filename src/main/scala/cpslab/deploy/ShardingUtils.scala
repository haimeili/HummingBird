package cpslab.deploy

import java.io.File

import scala.util.Random

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.{ClusterSharding, ShardRegion}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

private[cpslab] object ShardingUtils {
  
  private var maxShardNum = -1
  private var maxEntryNum = -1
  private var lshInstance: LSH = _
  private var localShardingSystem: ActorSystem = _

  private val independentNamespaceEntryResolver: ShardRegion.IdExtractor = {
    case req @ SearchRequest(_, _) =>
      ("1", req)
    case shardAllocation @ ShardAllocation(_) =>
      ("1", shardAllocation)
  }
  
  private val independentNamespaceShardResolver: ShardRegion.ShardResolver = msg => msg match {
    case searchRequest @ SearchRequest(_, _) =>
      //TODO: assign to local shards
      Random.nextInt(maxShardNum).toString
    case shardAllocation @ ShardAllocation(_) =>
      val tableID = shardAllocation.shardsMap.keys
      // in independent namespace, we allow only one table in ShardAllocation Info
      require(tableID.size == 1)
      tableID.toList(0).toString
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
    localShardingSystem = ActorSystem("LSH", conf)
    initShardAllocation(conf, lsh)
    require(maxEntryNum > 0 && maxShardNum > 0 & lshInstance != null,
      "please run ShardingUtils.initShardAllocation before you start Cluster Sharding System")
    
    // resolve different shard/entry resolver
    val (shardResolver, entryResolver) = conf.getString("cpslab.lsh.sharding.namespace") match {
      case "independent" => 
        // allowing only one entryactor in independent namespace
        require(maxEntryNum == 1)
        (independentNamespaceShardResolver, independentNamespaceEntryResolver)
    }
    
    ClusterSharding(localShardingSystem).start(
      typeName = ShardDatabaseWorker.shardDatabaseWorkerActorName,
      entryProps = entryProps,
      idExtractor = entryResolver,
      shardResolver = shardResolver
    )
    Thread.sleep(10000)
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

