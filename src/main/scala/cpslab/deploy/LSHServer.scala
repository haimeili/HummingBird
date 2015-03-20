package cpslab.deploy

import java.io.File

import scala.collection.mutable.ListBuffer

import akka.actor.{ActorSystem, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.contrib.pattern.ClusterSharding
import akka.routing.{BroadcastGroup, RoundRobinGroup}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.plsh.PLSHWorker
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

private[cpslab] object LSHServer {

  /**
   * start actor system for PLSH schema
   * @param conf the config object
   * @param lsh LSH instance to be passed to each worker
   * @param newActorProps the function generating the actor props
   * @return ActorSystem
   */
  private[deploy] def startPLSHSystem(conf: Config, lsh: LSH, 
      newActorProps: (Int, Config, LSH) => Props): ActorSystem = {
    // start actorSystem
    val system = ActorSystem("LSH", conf)
    // start local actors
    val localActorNum = conf.getInt("cpslab.lsh.plsh.localActorNum")
    // local ID
    val localNodeId = conf.getInt("cpslab.lsh.nodeID")

    for (i <- 0 until localActorNum) {
      val props = newActorProps(localNodeId * localActorNum + i, conf, lsh)
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

  /**
   * start Actor system for cluster sharding schema
   * @param conf the config object
   * @param lsh LSH instance
   */
  private[deploy] def startShardingSystem(conf: Config, lsh: LSH): ActorSystem = {
    val (_, system) = ShardingUtils.startShardingSystem(
      Some(Props(new ShardDatabaseWorker(conf, lsh))),
      conf, lsh)

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
    val router = system.actorOf(
      ClusterRouterGroup(
        local = RoundRobinGroup(List(shardRegionActorPath)),
        settings = ClusterRouterGroupSettings(
          totalInstances = clientHandlerNumber,
          routeesPaths = List(shardRegionActorPath),
          allowLocalRoutees = true,
          useRole = Some("compute"))).props(),
      name = "clientRequestHandler")
    system
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
    val lshEngine = new LSH(conf)
    val system = conf.getString("cpslab.lsh.distributedSchema") match {
      case "PLSH" =>
        startPLSHSystem(conf, lshEngine, PLSHWorker.props)
      case "SHARDING" => 
        startShardingSystem(conf, lshEngine)
      case x => 
        println(s"Unsupported Distributed Schema $x")
        null
    }
    
    val regionActor = ClusterSharding(system).shardRegion(
      ShardDatabaseWorker.shardDatabaseWorkerActorName)
    //regionActor ! SearchRequest("vector0", new SparseVector(3, Array(0, 1), Array(1.0, 1.0)))
  }
}

