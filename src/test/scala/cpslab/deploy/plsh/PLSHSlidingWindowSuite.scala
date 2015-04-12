package cpslab.deploy.plsh

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy.LSHServer
import cpslab.deploy.plsh.benchmark.PLSHClientActor
import cpslab.lsh.LSH
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

class PLSHSlidingWindowSuite(var actorSystemPair: (ActorSystem, ActorSystem))
  extends TestKit(actorSystemPair._1) with ImplicitSender with FunSuiteLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    actorSystemPair._1.shutdown()
    actorSystemPair._2.shutdown()
  }

  def this() = this({
    val familyFile = s"${getClass.getClassLoader.getResource("testprecalculated").getFile}," +
      s"${getClass.getClassLoader.getResource("testprecalculated_pstable").getFile}"
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.nodeID=0
         |akka.remote.netty.tcp.port=3000
         |akka.cluster.seed-nodes = ["akka.tcp://LSH@127.0.0.1:3000"]
         |cpslab.lsh.plsh.maxNumberOfVector=2
         |cpslab.lsh.plsh.updateWindowSize=1
         |cpslab.lsh.plsh.mergeThreshold=2
         |cpslab.lsh.name=precalculated
         |cpslab.lsh.familyFilePath="$familyFile"
         |cpslab.lsh.generateMethod=fromfile
         |cpslab.lsh.tableNum = 9
      """.stripMargin).withFallback(TestSettings.testBaseConf)
    val lsh = new LSH(conf)
    val conf1 = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.nodeID=1
         |akka.cluster.seed-nodes = ["akka.tcp://LSH@127.0.0.1:3000"]
         |akka.remote.netty.tcp.port=3001
         |cpslab.lsh.plsh.maxNumberOfVector=2000
         |cpslab.lsh.plsh.updateWindowSize=1
         |cpslab.lsh.plsh.mergeThreshold=2
         |cpslab.lsh.name=precalculated
         |cpslab.lsh.familyFilePath="$familyFile"
         |cpslab.lsh.generateMethod=fromfile
         |cpslab.lsh.tableNum = 9
      """.stripMargin).withFallback(TestSettings.testBaseConf)
    val system0 = LSHServer.startPLSHSystem(conf, lsh, PLSHWorker.props)
    Thread.sleep(5000)
    val system1 = LSHServer.startPLSHSystem(conf1, lsh, PLSHWorker.props)
    Thread.sleep(5000)
    (system0, system1)
  })

  test("PLSH update sliding window correctly") {
    val slidingWindowVectorFilePath = getClass.getClassLoader.getResource("slidingwindowtestfile").getFile
    val conf = ConfigFactory.parseString(
      s"""
        |cpslab.lsh.plsh.benchmark.inputSource=$slidingWindowVectorFilePath
      """.stripMargin).withFallback(TestSettings.testClientConf)
    //initialize update window
    val benchmarkClient = TestActorRef[PLSHClientActor](new PLSHClientActor(conf))
    Thread.sleep(5000)
    assert(benchmarkClient.underlyingActor.currentLowerBound === 1)
    assert(benchmarkClient.underlyingActor.currentUpperBound === 1)
  }

}
