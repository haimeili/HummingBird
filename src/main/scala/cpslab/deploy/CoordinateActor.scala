package cpslab.deploy

import java.io.File

import akka.routing.FromConfig
import com.typesafe.config.ConfigFactory
import cpslab.util.Configuration

import scala.collection.mutable.{HashMap, ListBuffer}

import akka.actor._
import org.apache.spark.mllib.linalg.SparseVector

class CoordinateActor(configuration: Configuration) extends Actor {

  val workerRouter = context.actorOf(
    FromConfig.props(Props(classOf[WorkerActor], configuration)),
    name = "workerActorRouter")

  private val hashTableNum = configuration.getInt("cpslab.hashtableNum")

  override def receive: Receive = {
    case Register(execId, url) =>
      // TODO
    case req @ QueryRequest(_) =>
      workerRouter ! req
    case QueryResponse(vector: Array[SparseVector]) =>
      // TODO: provide forward to the client or provide some APIs with Await.result()
    case Heartbeat(id: String, responseTime: Long) =>
      // TODO: accumulate the status of coordinators and periodically check if need the
      // worker to increase shard
      sender ! IncreaseShard
    case Insert(vector: SparseVector) =>
      // TODO: insert logic
  }
}

object CoordinateActor {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("Usage: program configFilePath")
      System.exit(1)
    }
    val coordinatorConfig = ConfigFactory.load("coordinate")
    val configInstance = new Configuration(coordinatorConfig)
    // build the actor system
    val system = ActorSystem("CoordinatorClusterSystem", coordinatorConfig)
    system.actorOf(Props[CoordinateActor], name = "coordinator")
  }
}
