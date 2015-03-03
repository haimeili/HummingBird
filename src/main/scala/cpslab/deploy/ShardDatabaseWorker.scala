package cpslab.deploy

import akka.actor.Actor
import com.typesafe.config.Config

private[deploy] class ShardDatabaseWorker(conf: Config) extends Actor{
  
  override def receive: Receive = {
    null  
  }
}

private[deploy] object ShardDatabaseWorker {
  val ShardDatabaseWorkerActorName = "ShardDatabaseWorker"
}

