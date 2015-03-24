package cpslab.deploy

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.ClusterSharding
import akka.testkit._
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
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
         |cpslab.lsh.sharding.systemName = "IndependentShardingSystem"
         |akka.cluster.seed-nodes = ["akka.tcp://IndependentShardingSystem@127.0.0.1:2554"]
         |akka.remote.netty.tcp.port = 2554
         |cpslab.lsh.sharding.namespace = independent
       """.stripMargin).withFallback(TestSettings.testShardingConf)
    LSHServer.startShardingSystem(conf, new DummyLSH(conf))
    })

  
  test("(independent) Sharding scheme forwards search request correctly") {
    val client = TestActorRef[Client](Props(new Client), name = "client")
    val clientHandler = ClusterSharding(actorSystem).shardRegion(
      ShardDatabaseWorker.shardDatabaseWorkerActorName)
    clientHandler ! SearchRequest(0, new SparseVector(3, Array(0, 1), Array(1.0, 1.0)))
    clientHandler ! SearchRequest(1, new SparseVector(3, Array(0, 1), Array(1.0, 1.0)))
    Thread.sleep(2000)
    val checkResult = {
      if (client.underlyingActor.state.contains(1)) {
        client.underlyingActor.state(1).toList == List[Long](0L)
      } else if (client.underlyingActor.state.contains(0)) {
        client.underlyingActor.state(0).toList == List[Long](1L)
      } else {
        false
      }
    }
    assert(checkResult === true)
  }
}
