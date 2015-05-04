package cpslab.deploy

import akka.actor.{ActorSystem, Props}
import akka.contrib.pattern.ClusterSharding
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy.utils.{Client, DummyLSH}
import cpslab.lsh.vector.SparseVector
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class FlatShardingSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfterAll {

  import ShardDatabase._

  override def beforeAll(): Unit = {
    ShardDatabase.initializeMapDBHashMap(actorSystem.settings.config)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def this() = this({
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.sharding.systemName = "FlatShardingSystem"
         |akka.cluster.seed-nodes = ["akka.tcp://FlatShardingSystem@127.0.0.1:2553"]
         |akka.remote.netty.tcp.port = 2553
         |cpslab.lsh.sharding.namespace = flat
       """.stripMargin).withFallback(TestSettings.testShardingConf)
    LSHServer.startShardingSystem(conf, new DummyLSH(conf))
  })


  test("(flat) Sharding scheme forwards search request correctly") {
    val client = TestActorRef[Client](Props(new Client), name = "client")
    val clientHandler = ClusterSharding(actorSystem).shardRegion(
      ShardDatabaseWorker.shardDatabaseWorkerActorName)
    clientHandler ! SearchRequest(new SparseVector(0, 3, Array(0, 1), Array(1.0, 1.0)))
    Thread.sleep(2000)
    clientHandler ! SearchRequest(new SparseVector(1, 3, Array(0, 1), Array(1.0, 1.0)))
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
