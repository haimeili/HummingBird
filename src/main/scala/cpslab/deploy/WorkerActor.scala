package cpslab.deploy

import scala.language.postfixOps
import scala.concurrent.duration._

import akka.util.Timeout
import akka.actor._
import akka.contrib.pattern.ShardRegion.Passivate
import akka.pattern.ask
import akka.contrib.pattern.{DistributedPubSubExtension, ClusterSharding, ShardRegion}
import akka.contrib.pattern.DistributedPubSubMediator.{Subscribe, SubscribeAck}
import akka.persistence.PersistentActor
import com.typesafe.config.ConfigFactory

import cpslab.lsh.{LSH, LSHFactory}
import cpslab.util.Configuration

class WorkerActor(configuration: Configuration) extends PersistentActor with ActorLogging {

  // configure the pub-sub service
  val mediator = DistributedPubSubExtension(context.system).mediator

  // subscribe the topics
  configuration.getString("cpslab.worker.subscribeTopic").split(",").foreach(topic => {
      mediator ! Subscribe(topic, self) // assume the group is null
    }
  )

  override def persistenceId: String = self.path.parent.name + "-" + self.path.name

  // passivate the entity when no activity
  context.setReceiveTimeout(2.minutes)

  WorkerActor.lshInstance = LSHFactory(configuration).newInstance()

  // TODO: define the state

  override def receiveRecover: Receive = {
    // TODO: recovery logic
    null
  }

  override def receiveCommand: Receive = {
    case Init(_) =>
      sender ! true
    case SubscribeAck(Subscribe("query", None, `self`)) =>
      context.become(receiveQueryOrInsert)
    case x =>
      log.warning("received unknown message " + x)
  }

  private def receiveQueryOrInsert: Receive = {
    case QueryRequest(shardId, vector) =>
      // query something
      val similarVectors = WorkerActor.lshInstance.queryData(vector)
      // send back the result
      sender() ! QueryResponse(similarVectors)
    case InsertRequest(_, vector) =>
      // insert a new vector
      WorkerActor.lshInstance.insertData(vector)
    case x =>
      log.warning("received unknown message " + x)
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

  private[deploy] val shardName = "LSHWorker"

  private var lshInstance: LSH = null

  // TODO: make the maximum number of the actors configurable
  val idExtractor: ShardRegion.IdExtractor = {
    case q @ QueryRequest(id, vector) => (
      (math.abs(vector.hashCode()) % 100).toString, q)
    case i @ InsertRequest(id, vector) => (
      (math.abs(vector.hashCode()) % 100).toString, i)
  }

  val shardResolver: ShardRegion.ShardResolver = msg => msg match {
    case QueryRequest(id, vector) => id.toString
    case InsertRequest(id, vector) => id.toString
  }

  def props(config: Configuration): Props = Props(new WorkerActor(config))

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("Usage: program configFilePath")
      System.exit(1)
    }
    val config = ConfigFactory.load("worker")
    val configInstance = new Configuration(config)
    // build the actor system
    val system = ActorSystem("WorkerClusterSystem", config)

    val minEntryNum = configInstance.getInt("cpslab.worker.minEntryNum")
    val maxEntryNum = configInstance.getInt("cpslab.worker.maxEntryNum")
    val initTimeout = configInstance.getInt("cpslab.worker.initTimeout")

    //TODO: start persistence journal

    val lshRegion = ClusterSharding(system).start(
      typeName = shardName,
      entryProps = Some(props(configInstance)),
      idExtractor = idExtractor,
      shardResolver = shardResolver
    )

    implicit val timeout = Timeout(initTimeout seconds)
    // initialize the entry actor with ask pattern
    for (i <- 0 until minEntryNum) {
      if (!(lshRegion ? Init(i)).asInstanceOf[Boolean]) {
        throw new Exception("cannot initialize actor")
      }
    }
  }
}
