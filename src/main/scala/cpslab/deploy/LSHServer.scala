package cpslab.deploy

import java.io.File

import scala.collection.mutable.ListBuffer

import akka.actor.{ActorSystem, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.contrib.pattern.ClusterSharding
import akka.routing.{BroadcastGroup, RoundRobinGroup}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.plsh.PLSHWorker
import cpslab.lsh.{LSH, LSHFactory}

private[cpslab] object LSHServer {
  
  private def startPLSHSystem(conf: Config, lsh: LSH): Unit = {
    // start actorSystem
    val system = ActorSystem("LSHSystem")
    // start local actors
    val localActorNum = conf.getInt("cpslab.lsh.plsh.localActorNum")

    for (i <- 0 until localActorNum) {
      system.actorOf(Props(new PLSHWorker(conf = conf, lshInstance = lsh)),
        name = s"LocalActor-$i")
    }

    //start clientRequestHandler
    val routeePath = {
      val t = new ListBuffer[String]
      for (i <- 0 until localActorNum) {
        t += s"/user/LocalActor-$i"
      }
      t.toList
    }
    system.actorOf(
      props = ClusterRouterGroup(
        local = BroadcastGroup(routeePath),
        settings = ClusterRouterGroupSettings(
            totalInstances = 1, 
            routeesPaths = routeePath, 
            allowLocalRoutees = true, 
            useRole = Some("compute"))).props(),
      name = "clientRequestHandler")
  }
  
  private def startShardingSystem(conf: Config, lsh: LSH): Unit = {
    val (_, system) = CommonUtils.startShardingSystem(
      Some(Props(new ShardDatabaseWorker(conf, lsh))),
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
    conf.getString("cpslab.lsh.distributedSchema") match {
      case "PLSH" => startPLSHSystem(conf, lshInstance.get)
      case "SHARDING" => startShardingSystem(conf, lshInstance.get)
      case x => println(s"Unsupported Distributed Schema $x")
    }
  }
}

