package cpslab.deploy

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.contrib.pattern.{ShardRegion, ClusterSharding}
import akka.contrib.pattern.ShardRegion.Passivate
import akka.persistence.PersistentActor
import akka.routing.FromConfig
import com.google.common.util.concurrent
import com.google.common.util.concurrent.AtomicDouble
import com.typesafe.config.ConfigFactory
import cpslab.lsh.{LSH, LSHFactory}
import cpslab.util.{RingBuffer, Configuration}

class WorkerActor(configuration: Configuration) extends PersistentActor with ActorLogging {

  val shardRouter = context.actorOf(
    FromConfig.props(Props(classOf[WorkerActor], configuration)),
    name = "workerActorRouter")

  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  // TODO: caculate the streaming window average response time based on time window
  // using ringBuffer to implement
  private var averageResponseTime: Double = 0.0 //ms
  private val responseTimeBuffer = new RingBuffer[Double]

  WorkerActor.lshInstance = LSHFactory(configuration).newInstance()

  override def preStart() = {
    //TODO: periodically send the heartbeat to the coordinator
  }

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
    val coordinatorConfig = ConfigFactory.load("worker")
    val configInstance = new Configuration(coordinatorConfig)
    // build the actor system
    val system = ActorSystem("WorkerClusterSystem", coordinatorConfig)
    system.actorOf(Props[WorkerActor], name = "worker")
  }
}
