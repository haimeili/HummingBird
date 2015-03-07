package cpslab.deploy

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.lsh.LSH

//messages for testing
case object Ping
case object Pong

class DummyPLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {

  override def receive: Receive = {
    case Ping =>
      sender ! Pong
  }
}

object DummyPLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH) = {
    Props(new DummyPLSHWorker(id, conf, lshInstance))
  }
}
