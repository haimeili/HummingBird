package cpslab.deploy

import scala.collection.mutable

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.lsh.vector.{SparseVector, SparseVectorWrapper}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuiteLike}

class ShardDatabaseStorageSuite extends TestKit(ActorSystem())
    with ImplicitSender with FunSuiteLike with BeforeAndAfter with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  test ("(flat) ShardDatabaseStorage calculate the similarity, index new vector correctly") {
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.deploy.client = ${testActor.path.toStringWithoutAddress}
         |cpslab.lsh.sharding.namespace = flat
      """.stripMargin).withFallback(TestSettings.testShardingConf)
    val databaseNode = TestActorRef[ShardDatabaseStorage](ShardDatabaseStorage.props(conf))
    val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
    val byteArray = Array.fill[Int](1)(0)
    indexMap += 0 -> List(SparseVectorWrapper(byteArray,
      new SparseVector(0, 3, Array(0, 1), Array(1.0, 1.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    indexMap.remove(0)
    indexMap += 0 -> List(SparseVectorWrapper(byteArray,
      new SparseVector(1, 3, Array(0, 1), Array(1.0, 2.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    assert(databaseNode.underlyingActor.elementsInFlatSpace.get(0).get.get(0).get.toList ===
      List(new SparseVector(0, 3, Array(0, 1), Array(1.0, 1.0)),
        new SparseVector(1, 3, Array(0, 1), Array(1.0, 2.0))))
  }
}
