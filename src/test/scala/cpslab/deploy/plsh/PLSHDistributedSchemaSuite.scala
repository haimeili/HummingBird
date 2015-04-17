package cpslab.deploy.plsh

import scala.language.postfixOps

import akka.actor.{ActorSystem, InvalidActorNameException, Props}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import cpslab.TestSettings
import cpslab.deploy.LSHServer
import cpslab.deploy.utils.DummyPLSHWorker
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

/**
 * testing whether the distribution PLSH is started correctly
 * @param actorSystem the actor system of PLSH
 */
class PLSHDistributedSchemaSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfterAll {
  
  def this() = this({
    val conf = TestSettings.testBaseConf
    LSHServer.startPLSHSystem(conf, null, DummyPLSHWorker.props)
  })
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  test("LSHServer starts PLSH system and the actors correctly") {
    intercept[InvalidActorNameException] {
      actorSystem.actorOf(Props(new DummyPLSHWorker(0, ConfigFactory.load(), null)),
        name = s"PLSHWorker")
    }
  }
}
