package cpslab.deploy.plsh.benchmark

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.plsh.{CapacityFullNotification, WindowUpdate, WindowUpdateNotification}
import cpslab.deploy.{IOTicket, SearchRequest}
import cpslab.lsh.vector.{SparseVector, Vectors}

private[plsh] class PLSHClientActor(conf: Config) extends Actor {

  private val inputSource = conf.getString("cpslab.lsh.plsh.benchmark.inputSource")
  private val remoteActorList = conf.getStringList("cpslab.lsh.plsh.benchmark.remoteProxyList")
  private val sendInterval = conf.getLong("cpslab.lsh.plsh.benchmark.messageInterval")

  private val remoteProxies: List[ActorSelection] = connectToRemoteProxies()

  private var benchmarkTask: Cancellable = null
  private val queries: ListBuffer[SparseVector] = new ListBuffer[SparseVector]
  private var queryIndex = 0

  private[plsh] var currentLowerBound = 0
  private[plsh] var currentUpperBound = 0
  private var slidingWindowInitialized = false

  private def connectToRemoteProxies(): List[ActorSelection] = {
    val connectedRemoteProxies = new ListBuffer[ActorSelection]
    for (remoteProxyAddress <- remoteActorList) {
      connectedRemoteProxies += context.actorSelection(remoteProxyAddress)
    }
    connectedRemoteProxies.toList
  }

  override def preStart(): Unit = {
    val system = context.system
    import system.dispatcher
    // load the actors
    for (line <- Source.fromFile(inputSource).getLines()) {
      queries += new SparseVector(Vectors.fromString(line))
    }
    val a = Random.nextInt(remoteProxies.length)
    println("sending to proxy " + a)
    remoteProxies(a) ! WindowUpdate(currentLowerBound, currentUpperBound)
    benchmarkTask = system.scheduler.schedule(0 milliseconds, sendInterval milliseconds, self,
      IOTicket)
  }

  override def receive: Receive = {
    case CapacityFullNotification(id) =>
      if (id >= currentLowerBound && id <= currentUpperBound) {
        remoteProxies(Random.nextInt(remoteProxies.length)) !
          WindowUpdate(currentLowerBound + 1, currentUpperBound + 1)
        currentLowerBound += 1
        currentUpperBound += 1
      }
    case WindowUpdateNotification(id) =>
      if (!slidingWindowInitialized) {
        slidingWindowInitialized = true
      }
    case IOTicket =>
      if (slidingWindowInitialized) {
        val selectedProxyIndex = Random.nextInt(remoteProxies.length)
        val selectedProxyAddress = remoteProxies(selectedProxyIndex)
        selectedProxyAddress ! SearchRequest(queries(queryIndex))
        queryIndex += 1
        if (queryIndex > queries.length - 1) {
          if (benchmarkTask != null) {
            benchmarkTask.cancel()
          }
        }
      }
  }
}

private[plsh] object PLSHClientActor {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: program client_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(0)))
    val actorSystem = ActorSystem("PLSHBenchmark", conf)
    actorSystem.actorOf(Props(new PLSHClientActor(conf)))
  }
}
