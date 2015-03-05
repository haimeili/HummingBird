package cpslab.deploy.plsh

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.lsh.LSH

class PLSHWorker(conf: Config, lshInstance: LSH) extends Actor {
  override def receive: Receive = {
    null
  }
}


object PLSHWorker {
  def props(conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(conf, lshInstance))
  }
}
