package cpslab.deploy

import java.io.File

import akka.actor._
import akka.contrib.pattern.{ShardRegion, ClusterSharding}
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory

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

  private val shardName = "LSHWorker"

  val idExtractor: ShardRegion.IdExtractor = {
    // TODO: implement the shardResolver
    case msg: Message => (0.toString, msg)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    // TODO: implement the shardResolver
    case msg: Message => 0.toString
  }

  def props(): Props = Props(new WorkerActor)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: program configFilePath")
      System.exit(1)
    }
    val config = ConfigFactory.parseFile(new File(args(0)))
    // build the actor system
    val system = ActorSystem("ClusterSystem", config)
    val lshRegion = ClusterSharding(system).start(
      typeName = shardName,
      entryProps = Some(WorkerActor.props()),
      idExtractor = WorkerActor.idExtractor,
      shardResolver = WorkerActor.shardResolver)
  }
}
