package cpslab.deploy

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings}
import akka.contrib.pattern.ClusterSharding
import akka.routing.{BroadcastGroup, RoundRobinGroup}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.plsh.PLSHWorker
import cpslab.lsh.LSH

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
    // local ID
    val localNodeId = conf.getInt("cpslab.lsh.nodeID")

    val props = newActorProps(localNodeId, conf, lsh)
    system.actorOf(props, name = s"PLSHWorker")

    //start clientRequestHandler
    val clientHandlerNumber = conf.getInt("cpslab.lsh.deploy.maxNodeNum")
    val routeePath = List(s"/user/PLSHWorker")

    system.actorOf(
      props = ClusterRouterGroup(
        local = BroadcastGroup(routeePath),
        settings = ClusterRouterGroupSettings(
          totalInstances = clientHandlerNumber,
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
      Some(Props(new ShardDatabaseWorker(conf, lsh))), conf, lsh)

    val shardRegionActorPath = ClusterSharding(system).shardRegion(ShardDatabaseWorker.
      shardDatabaseWorkerActorName).path.toStringWithoutAddress
    val clientHandlerNumber = conf.getInt("cpslab.lsh.deploy.maxNodeNum")
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
    if (args.length != 1) {
      println("Usage: program conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(0)))

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
  }
}

