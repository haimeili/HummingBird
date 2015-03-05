package cpslab.deploy.plsh

import akka.actor.Actor
import com.typesafe.config.Config
import cpslab.lsh.LSH

class PLSHWorker(conf: Config, lshInstance: LSH) extends Actor {
  override def receive: Receive = ???
}
