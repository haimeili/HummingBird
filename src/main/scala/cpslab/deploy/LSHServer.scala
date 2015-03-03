package cpslab.deploy

import java.io.File

import akka.actor.Props
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.contrib.pattern.ClusterSharding
import akka.routing.RoundRobinGroup
import com.typesafe.config.ConfigFactory

class LSHServer {

}

object LSHServer {
  
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: program akka_conf_path app_conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(0))).
      withFallback(ConfigFactory.parseFile(new File(args(1)))).
      withFallback(ConfigFactory.load())
    val (_, system) = CommonUtils.startShardingSystem(Some(Props(new ShardDatabaseWorker(conf))),
      conf)
    val regionActorPath = ClusterSharding(system).shardRegion(ShardDatabaseWorker.
      shardDatabaseWorkerActorName).path.toStringWithoutAddress
    // start the router
    val routerActor = system.actorOf(
      ClusterRouterGroup(
        local = RoundRobinGroup(List(regionActorPath)), 
        settings = ClusterRouterGroupSettings(
          totalInstances = 100, 
          routeesPaths = List(regionActorPath),
          allowLocalRoutees = true, 
          useRole = Some("compute"))).props(), 
      name = "clientRequestHandler")
    println(routerActor.path.toString + " started")
  }
}

