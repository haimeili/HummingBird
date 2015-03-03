package cpslab.deploy

import java.io.File

import akka.actor.Props
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.contrib.pattern.ClusterSharding
import akka.routing.RoundRobinGroup
import com.typesafe.config.ConfigFactory
import cpslab.lsh.LSHFactory

private[cpslab] object LSHServer {
  
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: program akka_conf_path app_conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(0))).
      withFallback(ConfigFactory.parseFile(new File(args(1)))).
      withFallback(ConfigFactory.load())
    
    // initialize the LSH instance
    val lshInstance = LSHFactory.newInstance(conf.getString("cpslab.lsh.name"))
    require(lshInstance != None)
    
    val (_, system) = CommonUtils.startShardingSystem(
      Some(Props(new ShardDatabaseWorker(conf, lshInstance.get))),
      conf)
    
    val shardRegionActorPath = ClusterSharding(system).shardRegion(ShardDatabaseWorker.
      shardDatabaseWorkerActorName).path.toStringWithoutAddress
    val clientHandlerNumber = {
      try {
        conf.getInt("cpslab.lsh.deploy.clientHandlerInstanceNumber")
      } catch {
        case e: Exception => 10
      }
    }
    // start the router
    val routerActor = system.actorOf(
      ClusterRouterGroup(
        local = RoundRobinGroup(List(shardRegionActorPath)),
        settings = ClusterRouterGroupSettings(
          totalInstances = clientHandlerNumber,
          routeesPaths = List(shardRegionActorPath),
          allowLocalRoutees = true, 
          useRole = Some("compute"))).props(), 
      name = "clientRequestHandler")
    println(routerActor.path.toString + " started")
  }
}

