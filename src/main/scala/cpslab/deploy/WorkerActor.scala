package cpslab.deploy

import akka.actor.{PoisonPill, ReceiveTimeout, ActorLogging, Actor}
import akka.actor.Actor.Receive
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.PersistentActor

class WorkerActor extends PersistentActor with ActorLogging {

  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  // TODO: define the state

  override def receiveRecover: Receive = {
    // TODO: recovery logic
    null
  }


  override def receiveCommand: Receive = {
    // TODO: message processing logic
    null
  }

  override def unhandled(msg: Any): Unit = msg match {
    case ReceiveTimeout =>
      // make the parent to buffer the message to the new incarnation of the entry
      context.parent ! Passivate(stopMessage = PoisonPill)
    case _              =>
      super.unhandled(msg)
  }
}

object WorkerActor {

  def main(args: Array[String]): Unit = {
    //TODO: main program to start the worker

  }
}
