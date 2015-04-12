package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit._
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy.{SearchRequest, SimilarityIntermediateOutput}
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class PLSHWorkerSuite(var actorSystem: ActorSystem) extends TestKit(actorSystem) with ImplicitSender
  with FunSuiteLike with BeforeAndAfterAll {

  def this() = this({
    val familyFile = s"${getClass.getClassLoader.getResource("testprecalculated").getFile}," +
      s"${getClass.getClassLoader.getResource("testprecalculated_pstable").getFile}"
    val plshWorkerSuiteConf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.plsh.updateWindowSize=1
         |cpslab.lsh.plsh.mergeThreshold=2
         |cpslab.lsh.name=precalculated
         |cpslab.lsh.familyFilePath="$familyFile"
         |cpslab.lsh.generateMethod=fromfile
         |cpslab.lsh.tableNum = 9
      """.stripMargin).withFallback(TestSettings.testBaseConf)
    ActorSystem("LSH", plshWorkerSuiteConf)
  })

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  test("PLSHWorker saves and fetches data with two-level partitioned table correctly") {
    val plshWorkerSuiteConf = system.settings.config
    val lsh = new LSH(plshWorkerSuiteConf)
    val plshWorker = TestActorRef[PLSHWorker](PLSHWorker.props(0, plshWorkerSuiteConf, lsh))
    val staticTableLength = plshWorker.underlyingActor.twoLevelPartitionTable.length
    val deltaTableLength = plshWorker.underlyingActor.deltaTable.length
    val bucketOffsetTableLength = plshWorker.underlyingActor.bucketOffsetTable.length
    assert(staticTableLength === 9)
    assert(deltaTableLength === 9)
    assert(bucketOffsetTableLength === 9)
    assert(staticTableLength === deltaTableLength)
    assert(staticTableLength === bucketOffsetTableLength)
    plshWorker.underlyingActor.receive(WindowUpdate(0, 0))
    //vector 1
    val vector1 = new SparseVector(1, 3, Array(0, 1), Array(0.1, 0.2))
    //index vector 1
    plshWorker.underlyingActor.receive(SearchRequest(vector1))
    Thread.sleep(5000)
    //check the internal data structure
    //static table
    for (i <- 0 until staticTableLength) {
      assert(plshWorker.underlyingActor.twoLevelPartitionTable(i) === null)
      assert(plshWorker.underlyingActor.deltaTable(i).size === 1)
      assert(plshWorker.underlyingActor.bucketOffsetTable(i) === null)
    }
    assert(plshWorker.underlyingActor.elementCountInDeltaTable.get() === 1)
    //vector 2
    val vector2 = new SparseVector(5, 3, Array(0, 1), Array(0.1, 0.2))
    //index vector 2
    plshWorker.underlyingActor.receive(SearchRequest(vector2))
    Thread.sleep(5000)
    for (i <- 0 until staticTableLength) {
      assert(plshWorker.underlyingActor.twoLevelPartitionTable(i).length === 2)
      assert(plshWorker.underlyingActor.deltaTable(i).size === 0)
      assert(plshWorker.underlyingActor.bucketOffsetTable(i).size === 1)
    }
    plshWorker.stop()
  }

  test("PLSHWorker responses similarity vectors correctly") {
    val plshWorkerSuiteConf = system.settings.config
    val lsh = new LSH(plshWorkerSuiteConf)
    val plshWorker = system.actorOf(PLSHWorker.props(0, plshWorkerSuiteConf, lsh))
    plshWorker ! WindowUpdate(0, 0)
    Thread.sleep(3000)
    val receivedWindowUpdateNotice = receiveN(1)
    assert(receivedWindowUpdateNotice(0).isInstanceOf[WindowUpdateNotification])
    val windowUpdateNotice = receivedWindowUpdateNotice(0).asInstanceOf[WindowUpdateNotification]
    assert(windowUpdateNotice.id == 0)
    //vector 1
    val vector1 = new SparseVector(1, 3, Array(0, 1), Array(0.1, 0.2))
    //index vector 1
    plshWorker ! SearchRequest(vector1)
    Thread.sleep(5000)
    //vector 2
    val vector2 = new SparseVector(2, 3, Array(0, 1), Array(0.1, 0.2))
    //index vector 2
    plshWorker ! SearchRequest(vector2)
    Thread.sleep(5000)
    val receivedMessages = receiveN(1)
    assert(receivedMessages(0).isInstanceOf[SimilarityIntermediateOutput])
    val simOutput = receivedMessages(0).asInstanceOf[SimilarityIntermediateOutput]
    assert(simOutput.queryVectorID === 2)
    assert(simOutput.similarVectorPairs.length === 1)
    assert(simOutput.similarVectorPairs.head === Tuple2(1, 0.05000000000000001))
    //index vector 3
    val vector3 = new SparseVector(3, 3, Array(0, 1), Array(0.1, 0.2))
    plshWorker ! SearchRequest(vector3)
    Thread.sleep(5000)
    val receivedMessages2 = receiveN(1)
    for (receivedMessage <- receivedMessages2) {
      assert(receivedMessage.isInstanceOf[SimilarityIntermediateOutput])
      val simOutput = receivedMessage.asInstanceOf[SimilarityIntermediateOutput]
      assert(simOutput.queryVectorID === 3)
      assert(simOutput.similarVectorPairs.length === 2)
      assert(simOutput.similarVectorPairs.head === Tuple2(1, 0.05000000000000001))
      assert(simOutput.similarVectorPairs(1) === Tuple2(2, 0.05000000000000001))
    }
  }
}
