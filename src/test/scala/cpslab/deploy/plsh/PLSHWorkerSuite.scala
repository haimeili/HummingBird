package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.deploy._
import cpslab.deploy.utils.DummyLSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuiteLike}



class PLSHWorkerSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfter 
  with BeforeAndAfterAll {

  def this() = this({
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.name = none
         |cpslab.lsh.similarityThreshold = 0.0
         |akka.remote.netty.tcp.port = 0
         |cpslab.lsh.vectorDim = 3
         |cpslab.lsh.chainLength = 10
         |cpslab.lsh.familySize = 100
         |cpslab.lsh.topK = 2
         |akka.remote.netty.tcp.hostname = "127.0.0.1"
         |akka.remote.netty.tcp.port = 2556
         |akka.cluster.roles = [compute]
         |akka.cluster.seed-nodes = ["akka.tcp://LSH@127.0.0.1:2556"]
         |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
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
    plshWorker ! SearchRequest(1,
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(1.0)).asInstanceOf[SparseVector])
    var receivedMessages = receiveN(0)
    plshWorker ! SearchRequest(2,
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.5)).asInstanceOf[SparseVector])
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityIntermediateOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityIntermediateOutput]
      assert(similarityOutput.queryVectorID === 2)
      assert(similarityOutput.similarVectorPairs.size === 1)
      for ((similarVector, similarity) <- similarityOutput.similarVectorPairs) {
        assert(similarVector === 1)
        assert(similarity === 0.5)
      }
    }
    //output multiple vectors
    plshWorker ! SearchRequest(4,
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.3)).asInstanceOf[SparseVector])
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityIntermediateOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityIntermediateOutput]
      assert(similarityOutput.queryVectorID === 4)
      assert(similarityOutput.similarVectorPairs.size === 2)
    }
    // test topK
    plshWorker ! SearchRequest(3,
      Vectors.sparse(1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.8)).asInstanceOf[SparseVector])
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityIntermediateOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityIntermediateOutput]
      assert(similarityOutput.queryVectorID === 3)
      assert(similarityOutput.similarVectorPairs.size === 2)
      val (similarVector, similarity) = similarityOutput.similarVectorPairs.head
      assert(similarVector === 1)
      assert(similarity === 0.8)
    }
  }
}