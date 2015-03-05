package cpslab.deploy

import scala.language.postfixOps

import akka.actor.{ActorSystem, InvalidActorNameException, Props}
import akka.testkit._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}

/**
 * testing whether the distribution PLSH is started correctly
 * @param actorSystem the actor system of PLSH
 */
class PLSHDistributedSchemaSuite(var actorSystem: ActorSystem)
  extends TestKit(actorSystem) with ImplicitSender with FunSuiteLike with BeforeAndAfterAll {
  
  def this() = this({
    val conf = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.plsh.localActorNum = 10
       """.stripMargin)
    LSHServer.startPLSHSystem(conf, null, DummyPLSHWorker.props)
  })
  
  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }
  
  test("LSHServer start PLSH system and the actors correctly") {
    for (i <- 0 until 10) {
      intercept[InvalidActorNameException] {
        actorSystem.actorOf(Props(new DummyPLSHWorker(ConfigFactory.load(), null)), 
          name = s"PLSHWorker-$i")
      }
    }
    intercept[InvalidActorNameException] {
      actorSystem.actorOf(Props(new DummyPLSHWorker(ConfigFactory.load(), null)), 
        name = "clientRequestHandler")
    }
  }
  
  test("PLSH clientRequestHandler can broadcast the request to all machines correctly ") {
    val clientHandler = actorSystem.actorSelection("/user/clientRequestHandler")
    clientHandler ! Ping
    val receivedMessages = receiveN(10)
    for (msg <- receivedMessages) {
      assert(msg === Pong)
    }
  }
}
