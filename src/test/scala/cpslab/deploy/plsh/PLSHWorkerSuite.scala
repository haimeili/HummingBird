package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy._
import cpslab.deploy.utils.DummyLSH
import cpslab.lsh.vector.SparseVector
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuiteLike}

class PLSHWorkerSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfter
  with BeforeAndAfterAll {

  def this() = this({
    val plshWorkerSuiteConf = ConfigFactory.parseString(
      """
        |akka.remote.netty.tcp.port = 2556
        |akka.cluster.seed-nodes = ["akka.tcp://LSH@127.0.0.1:2556"]
        |cpslab.lsh.plsh.localActorNum = 10
      """.stripMargin).withFallback(TestSettings.testBaseConf)
    LSHServer.startPLSHSystem(plshWorkerSuiteConf, new DummyLSH(plshWorkerSuiteConf),
      PLSHWorker.props)
  })
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("PLSH saves vector and calculates (topK) similarity correctly (two-level partition)") {

  }

  test("PLSH saves vector and calculates (topK) similarity correctly (one-level partition)") {
    val plshWorker = actorSystem.actorSelection("/user/clientRequestHandler")
    plshWorker ! SearchRequest(new SparseVector(1, 1, Array.fill[Int](1)(0), Array.fill[Double](1)(1.0)))
    var receivedMessages = receiveN(0)
    plshWorker ! SearchRequest(new SparseVector(2, 1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.5)))
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
    plshWorker ! SearchRequest(new SparseVector(4, 1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.3)))
    receivedMessages = receiveN(10)
    for (i <- 0 until 10) {
      val receivedMessage = receivedMessages(i)
      assert(receivedMessage.isInstanceOf[SimilarityIntermediateOutput] === true)
      val similarityOutput = receivedMessage.asInstanceOf[SimilarityIntermediateOutput]
      assert(similarityOutput.queryVectorID === 4)
      assert(similarityOutput.similarVectorPairs.size === 2)
    }
    // test topK
    plshWorker ! SearchRequest(new SparseVector(3, 1, Array.fill[Int](1)(0), Array.fill[Double](1)(0.8)))
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