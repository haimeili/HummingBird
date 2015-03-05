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
  
  private[deploy] def startPLSHSystem(conf: Config, lsh: LSH, 
      newActorProps: (Config, LSH) => Props): ActorSystem = {
    // start actorSystem
    val system = ActorSystem("LSHSystem")
    // start local actors
    val localActorNum = conf.getInt("cpslab.lsh.plsh.localActorNum")

    for (i <- 0 until localActorNum) {
      val props = newActorProps(conf, lsh)
      system.actorOf(props, name = s"PLSHWorker-$i")
    }

    //start clientRequestHandler
    val routeePath = {
      val t = new ListBuffer[String]
      for (i <- 0 until localActorNum) {
        t += s"/user/PLSHWorker-$i"
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
    system
  }
  
  private[deploy] def startShardingSystem(conf: Config, lsh: LSH): Unit = {
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
    val lshEngine = LSHFactory.newInstance(conf.getString("cpslab.lsh.name"))
    require(lshEngine != None)
    conf.getString("cpslab.lsh.distributedSchema") match {
      case "PLSH" =>
        startPLSHSystem(conf, lshEngine.get, PLSHWorker.props)
      case "SHARDING" => startShardingSystem(conf, lshEngine.get)
      case x => println(s"Unsupported Distributed Schema $x")
    }
  }
}

