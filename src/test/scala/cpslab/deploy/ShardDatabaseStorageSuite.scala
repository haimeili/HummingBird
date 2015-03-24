package cpslab.deploy

import scala.collection.mutable

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.TestSettings
import cpslab.deploy.plsh.PLSHWorker
import cpslab.deploy.utils.DummyLSH
import cpslab.lsh.vector.{SparseVector, SparseVectorWrapper}
import cpslab.storage.ByteArrayWrapper
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuiteLike}

class ShardDatabaseStorageSuite extends TestKit(ActorSystem())
    with ImplicitSender with FunSuiteLike with BeforeAndAfter with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  test ("(independent) ShardDatabaseStorage calculate the similarity, index new vector correctly") {
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.deploy.client = ${testActor.path.toStringWithoutAddress}
         |cpslab.lsh.sharding.namespace = independent
      """.stripMargin).withFallback(TestSettings.testShardingConf)
    val databaseNode = TestActorRef[ShardDatabaseStorage](ShardDatabaseStorage.props(conf))
    val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
    val byteArray = Array.fill[Array[Byte]](1)(Array[Byte](0))
    indexMap += 0 -> List(SparseVectorWrapper(0, byteArray,
      new SparseVector(3, Array(0, 1), Array(1.0, 1.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    indexMap.remove(0)
    indexMap += 0 -> List(SparseVectorWrapper(1, byteArray,
      new SparseVector(3, Array(0, 1), Array(1.0, 2.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    assert(databaseNode.underlyingActor.elementsInIndependentSpace.get(
      ByteArrayWrapper(byteArray(0))).get.toList === List((0,
      new SparseVector(3, Array(0, 1), Array(1.0, 1.0))), (1,
      new SparseVector(3, Array(0, 1), Array(1.0, 2.0)))))
  }

  test ("(flat) ShardDatabaseStorage calculate the similarity, index new vector correctly") {
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.deploy.client = ${testActor.path.toStringWithoutAddress}
         |cpslab.lsh.sharding.namespace = flat
      """.stripMargin).withFallback(TestSettings.testShardingConf)
    val databaseNode = TestActorRef[ShardDatabaseStorage](ShardDatabaseStorage.props(conf))
    val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
    val byteArray = Array.fill[Array[Byte]](1)(Array[Byte](0))
    indexMap += 0 -> List(SparseVectorWrapper(0, byteArray,
      new SparseVector(3, Array(0, 1), Array(1.0, 1.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    indexMap.remove(0)
    indexMap += 0 -> List(SparseVectorWrapper(1, byteArray,
      new SparseVector(3, Array(0, 1), Array(1.0, 2.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    assert(databaseNode.underlyingActor.elementsInFlatSpace.get(0).get.
      get(ByteArrayWrapper(byteArray(0))).get.toList ===
      List((0, new SparseVector(3, Array(0, 1), Array(1.0, 1.0))),
        (1, new SparseVector(3, Array(0, 1), Array(1.0, 2.0)))))
  }
}
