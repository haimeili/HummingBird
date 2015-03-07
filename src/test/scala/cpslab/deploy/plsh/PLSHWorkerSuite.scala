package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy._
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuiteLike}

private class DummyLSH(conf: Config) extends LSH("", conf) {
  override def calculateIndex(vector: SparseVector, validTableIDs: Seq[Int]): Array[Array[Byte]] = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    // i.toByte ensure that the vector is distributed to all machines 
    (for (i <- 0 until tableNum) yield Array.fill[Byte](1)(i.toByte)).toArray
  }
}

class PLSHWorkerSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfter 
  with BeforeAndAfterAll {

  def this() = this({
    val conf = ConfigFactory.parseString(
      s"""
         |akka.remote.netty.tcp.port = 0
         |cpslab.lsh.vectorDim = 3
         |cpslab.lsh.chainLength = 10
         |cpslab.lsh.familySize = 100
         |cpslab.lsh.plsh.maxWorkerNum = 10
         |cpslab.lsh.tableNum = 10
         |cpslab.lsh.nodeID = 0
         |cpslab.lsh.plsh.localActorNum = 10
       """.stripMargin)
    LSHServer.startPLSHSystem(conf, new DummyLSH(conf), PLSHWorker.props)
  })
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  test("PLSH saves vector and calculates (topK) similarity correctly") {
    val plshWorker = actorSystem.actorSelection("/user/clientRequestHandler")
    plshWorker ! SearchRequest("vector1", 
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(1.0)).asInstanceOf[SparseVector])
    var receivedMessages = receiveN(0)
    plshWorker ! SearchRequest("vector2",
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.5)).asInstanceOf[SparseVector])
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityOutput]
      assert(similarityOutput.queryVectorID === "vector2")
      assert(similarityOutput.similarVectorPairs.get.size === 1)
      for ((similarVector, similarity) <- similarityOutput.similarVectorPairs.get) {
        assert(similarVector === "vector1")
        assert(similarity === 0.5)
      }
    }
    //output multiple vectors
    plshWorker ! SearchRequest("vector4",
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.3)).asInstanceOf[SparseVector])
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityOutput]
      assert(similarityOutput.queryVectorID === "vector4")
      assert(similarityOutput.similarVectorPairs.get.size === 2)
    }
    // test topK
    plshWorker ! SearchRequest("vector3",
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.8)).asInstanceOf[SparseVector], 1)
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityOutput]
      assert(similarityOutput.queryVectorID === "vector3")
      assert(similarityOutput.similarVectorPairs.get.size === 1)
      for ((similarVector, similarity) <- similarityOutput.similarVectorPairs.get) {
        assert(similarVector === "vector1")
        assert(similarity === 0.8)
      }
    }
  }
}