package cpslab.deploy

import scala.collection.mutable

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.plsh.PLSHWorker
import cpslab.lsh.vector.{SparseVectorWrapper, Vectors, SparseVector}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FunSuiteLike, FunSuite}

class ShardDatabaseStorageSuite(val setup: (Config, ActorSystem)) extends TestKit(setup._2) 
    with ImplicitSender with FunSuiteLike with BeforeAndAfter with BeforeAndAfterAll {

  def this() = this({
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.name = none
         |cpslab.lsh.similarityThreshold = 0.0
         |akka.remote.netty.tcp.port = 0
         |cpslab.lsh.vectorDim = 3
         |cpslab.lsh.topK = 1
         |cpslab.lsh.chainLength = 10
         |cpslab.lsh.familySize = 100
         |cpslab.lsh.plsh.maxWorkerNum = 10
         |cpslab.lsh.tableNum = 10
         |cpslab.lsh.nodeID = 0
         |cpslab.lsh.plsh.localActorNum = 10
       """.stripMargin)
    (conf, LSHServer.startPLSHSystem(conf, null, PLSHWorker.props))
  })
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  test ("(independent) ShardDatabaseStorage calculate the similarity, return results and " +
    "index new vector correctly") {
    val conf = setup._1.withFallback(
      ConfigFactory.parseString(s"cpslab.lsh.deploy.client = ${testActor.path.toStringWithoutAddress}"))
    val actorSystem = setup._2
    val databaseNode = TestActorRef[ShardDatabaseStorage](ShardDatabaseStorage.props(conf))
    val indexMap = new mutable.HashMap[Int, List[SparseVectorWrapper]]
    val byteArray = Array.fill[Array[Byte]](1)(Array[Byte](0))
    indexMap += 0 -> List(SparseVectorWrapper("0", byteArray, 
      new SparseVector(3, Array(0, 1), Array(1.0, 1.0))))
    databaseNode.underlyingActor.receive(LSHTableIndexRequest(indexMap))
    indexMap.remove(0)
    indexMap += 0 -> List(SparseVectorWrapper("1", byteArray,
      new SparseVector(3, Array(0, 1), Array(1.0, 2.0))))
    databaseNode ! LSHTableIndexRequest(indexMap)
    expectMsg(SimilarityOutput("1", List(("0", 3.0))))
    indexMap += 0 -> List(SparseVectorWrapper("2", byteArray,
      new SparseVector(3, Array(0, 1), Array(1.0, 3.0))))
    databaseNode ! LSHTableIndexRequest(indexMap)
    expectMsg(SimilarityOutput("2", List(("1", 7.0))))
  }
}
