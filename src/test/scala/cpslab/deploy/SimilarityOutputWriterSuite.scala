package cpslab.deploy

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.deploy.utils.Client
import cpslab.storage.LongBitSet
import org.scalatest.FunSuiteLike

class SimilarityOutputWriterSuite extends TestKit(ActorSystem()) with FunSuiteLike {

  test("SimilarityOutputWriter generates output correctly") {
    val clientActor = TestActorRef[Client](Props(new Client))
    val conf = ConfigFactory.parseString(
      s"""
        |cpslab.lsh.sharding.maxDatabaseNodeNum = 3
        |cpslab.lsh.topK = 2
        |cpslab.lsh.similarityThreshold = 0.0
        |cpslab.lsh.deploy.client = "${clientActor.path.toStringWithoutAddress}"
      """.stripMargin)
    val testActor = TestActorRef[SimilarityOutputWriter](Props(new SimilarityOutputWriter(conf)))
    testActor.underlyingActor.receive(SimilarityIntermediateOutput(0, new LongBitSet, List()))
    testActor.underlyingActor.receive(SimilarityIntermediateOutput(2, new LongBitSet, List()))
    testActor.underlyingActor.receive(SimilarityIntermediateOutput(3, new LongBitSet, List()))
    Thread.sleep(1000)
    val bitMap1 = new LongBitSet
    testActor.underlyingActor.receive(SimilarityIntermediateOutput(1, bitMap1,
      List((0, 0.1), (2, 0.1), (3, 0.1))))
    testActor.underlyingActor.receive(SimilarityIntermediateOutput(1, bitMap1,
      List((0, 0.1), (2, 0.1), (3, 0.1))))
    testActor.underlyingActor.receive(SimilarityIntermediateOutput(1, bitMap1,
      List((0, 0.1), (2, 0.1), (3, 0.1))))
    Thread.sleep(1000)
    assert(clientActor.underlyingActor.state.get(1).get.toList === List(0, 2))
  }
}
