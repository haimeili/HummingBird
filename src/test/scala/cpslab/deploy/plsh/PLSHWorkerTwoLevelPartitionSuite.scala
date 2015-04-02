package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy.SearchRequest
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class PLSHWorkerTwoLevelPartitionSuite extends TestKit(ActorSystem())
    with FunSuiteLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("PLSHWorker saves and fetches data with two-level partitioned table correctly") {
    val familyFile = s"${getClass.getClassLoader.getResource("testprecalculated").getFile}," +
      s"${getClass.getClassLoader.getResource("testprecalculated_pstable").getFile}"
    val plshWorkerSuiteConf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.name=precalculated
         |cpslab.lsh.familyFilePath="$familyFile"
         |cpslab.lsh.generateMethod=fromfile
         |akka.remote.netty.tcp.port = 2557
         |akka.cluster.seed-nodes = ["akka.tcp://LSH@127.0.0.1:2557"]
         |cpslab.lsh.plsh.localActorNum = 10
         |cpslab.lsh.tableNum = 9
         |cpslab.lsh.plsh.partitionSwitch=true
      """.stripMargin).withFallback(TestSettings.testBaseConf)
    val lsh = new LSH(plshWorkerSuiteConf)
    val plshWorker = TestActorRef[PLSHWorker](PLSHWorker.props(1, plshWorkerSuiteConf, lsh))
    val a1 = new SparseVector(1, 2, Array(0, 1), Array(1.0, 2.0))
    val b1 = 0.1
    val w1 = 5
    val a2 = new SparseVector(2, 2, Array(0, 1), Array(1.0, 3.0))
    val b2 = 0.2
    val w2 = 6
    val a3 = new SparseVector(3, 2, Array(0, 1), Array(1.0, 4.0))
    val b3 = 0.3
    val w3 = 6
    //vector 1
    val vector1 = new SparseVector(4, 2, Array(0, 1), Array(0.1, 0.2))
    //index 1
    val index1 = (SimilarityCalculator.fastCalculateSimilarity(vector1, a1) + b1) / w1
    plshWorker.underlyingActor.receive(SearchRequest(vector1))
    //check the internal data structure
    assert(plshWorker.underlyingActor.twoLevelPartitionTable.size === 2)
    for (firstLevelTable <- plshWorker.underlyingActor.twoLevelPartitionTable) {
      assert(firstLevelTable._2.size === 2)
    }
  }
}
