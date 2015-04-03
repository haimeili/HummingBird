package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy.SearchRequest
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class PLSHWorkerSuite extends TestKit(ActorSystem())
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
         |cpslab.lsh.tableNum = 9
      """.stripMargin).withFallback(TestSettings.testBaseConf)
    val lsh = new LSH(plshWorkerSuiteConf)
    val plshWorker = TestActorRef[PLSHWorker](PLSHWorker.props(1, plshWorkerSuiteConf, lsh))
    //vector 1
    val vector1 = new SparseVector(4, 2, Array(0, 1), Array(0.1, 0.2))
    //index 1
    plshWorker.underlyingActor.receive(SearchRequest(vector1))
    Thread.sleep(5000)
    //check the internal data structure
    assert(plshWorker.underlyingActor.twoLevelPartitionTable.length === 9)
    //TODO: wait for merge happen; check bucketIndexOffsetTable; check twoLevelPartitionTable
  }
}
