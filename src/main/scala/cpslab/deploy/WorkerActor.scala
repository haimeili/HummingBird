package cpslab.deploy

import java.io.File

import akka.actor._
import akka.contrib.pattern.{ShardRegion, ClusterSharding}
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory
import cpslab.lsh.{LSH, LSHFactory}
import cpslab.util.Configuration

class WorkerActor(conf: Configuration) extends PersistentActor with ActorLogging {

  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  WorkerActor.lshInstance = LSHFactory(conf).newInstance()

  // TODO: define the state

  override def receiveRecover: Receive = {
    // TODO: recovery logic
    null
  }


  override def receiveCommand: Receive = {
    // TODO: message processing logic
    case QueryRequest(vector) =>
      // query something
      val similarVectors = WorkerActor.lshInstance.queryData(vector)
      // send back the result
      sender() ! QueryResponse(similarVectors)
    case _ =>
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

  private var lshInstance: LSH = null

  val idExtractor: ShardRegion.IdExtractor = {
    // TODO: implement the shardResolver
    case msg: Message => (0.toString, msg)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    // TODO: implement the shardResolver
    case QueryRequest(vector) => ""
  }

  def props(config: Configuration): Props = Props(new WorkerActor(config))

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: program configFilePath")
      System.exit(1)
    }
    val config = ConfigFactory.parseFile(new File(args(0)))
    val configInstance = new Configuration(config)
    // build the actor system
    val system = ActorSystem("ClusterSystem", config)
    val lshRegion = ClusterSharding(system).start(
      typeName = shardName,
      entryProps = Some(WorkerActor.props(configInstance)),
      idExtractor = WorkerActor.idExtractor,
      shardResolver = WorkerActor.shardResolver)
  }
}
