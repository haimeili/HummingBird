package cpslab.deploy

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.ClusterSharding
import akka.testkit._
import com.typesafe.config.ConfigFactory
import cpslab.deploy.utils.{Client, DummyLSH}
import cpslab.lsh.vector.SparseVector
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class IndependentShardingSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  def this() = this({
    val conf = ConfigFactory.parseString(
      s"""
         |akka.loglevel = "INFO"
         |akka.cluster.roles = [compute]
         |akka.cluster.seed-nodes = ["akka.tcp://LSH@127.0.0.1:2553"]
         |cpslab.lsh.name = none
         |cpslab.lsh.familySize = 100
         |cpslab.lsh.topK = 10
         |cpslab.lsh.vectorDim = 3
         |cpslab.lsh.deploy.client = "/user/client"
         |cpslab.lsh.chainLength = 3
         |cpslab.lsh.distributedSchema = SHARDING
         |cpslab.lsh.sharding.namespace = independent
         |cpslab.lsh.sharding.maxShardNumPerTable = 100
         |cpslab.lsh.sharding.maxShardDatabaseWorkerNum = 1
         |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
         |akka.cluster.auto-down-unreachable-after = 10s
         |akka.remote.netty.tcp.hostname = "127.0.0.1"
         |akka.remote.netty.tcp.port = 2553
         |cpslab.lsh.sharding.maxDatabaseNodeNum = 1
         |akka.persistence.snapshot-store.local.dir = "target/snapshots-ClusterShardingSpec"
         |cpslab.lsh.similarityThreshold = 0.0
         |cpslab.lsh.nodeID = 0
         |cpslab.lsh.generateMethod = default
         |cpslab.lsh.name = pStable
         |cpslab.lsh.familySize = 10
         |cpslab.lsh.tableNum = 1
         |cpslab.lsh.vectorDim = 3
         |cpslab.lsh.chainLength = 2
         |cpslab.lsh.family.pstable.mu = 0.0
         |cpslab.lsh.family.pstable.sigma = 0.02
         |cpslab.lsh.family.pstable.w = 3
       """.stripMargin)
    LSHServer.startShardingSystem(conf, new DummyLSH(conf))
    })

  
  test("(independent) Sharding scheme forwards search request correctly") {
    val client = TestActorRef[Client](Props(new Client), name = "client")
    val clientHandler = ClusterSharding(actorSystem).shardRegion(
      ShardDatabaseWorker.shardDatabaseWorkerActorName)
    clientHandler ! SearchRequest("vector0", new SparseVector(3, Array(0, 1), Array(1.0, 1.0)))
    clientHandler ! SearchRequest("vector1", new SparseVector(3, Array(0, 1), Array(1.0, 1.0)))
    Thread.sleep(2000)
    val checkResult = {
      if (client.underlyingActor.state.contains("vector1")) {
        client.underlyingActor.state("vector1") == List(("vector0", 2.0))
      } else if (client.underlyingActor.state.contains("vector0")) {
        client.underlyingActor.state("vector0") == List(("vector1", 2.0))
      } else {
        false
      }
    }
    assert(checkResult === true)
  }
}
