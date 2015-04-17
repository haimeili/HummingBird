package cpslab.deploy.plsh.benchmark

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.plsh.{CapacityFullNotification, WindowUpdate, WindowUpdateNotification}
import cpslab.deploy.{IOTicket, SearchRequest, SimilarityOutput}
import cpslab.lsh.vector.{SparseVector, Vectors}

private[plsh] class PLSHClientActor(conf: Config) extends Actor {

  private val inputSource = conf.getString("cpslab.lsh.plsh.benchmark.inputSource")
  private val sendInterval = conf.getLong("cpslab.lsh.plsh.benchmark.messageInterval")

  private var benchmarkTask: Cancellable = null
  private val queries: ListBuffer[SparseVector] = new ListBuffer[SparseVector]
  private var queryIndex = 0

  private[plsh] var currentLowerBound = 0
  private[plsh] var currentUpperBound = 0
  // vectorID -> Long
  private val readTimeMeasurementResults = new ListBuffer[(Int, Long)]

  //worker list
  private val workersList = conf.getStringList("cpslab.lsh.plsh.workerList")
  private val actors = {
    for (workerAddress <- workersList)
      yield context.actorSelection(workerAddress)
  }
  private val slidingWindowInitializedFlag = Array.fill[Boolean](actors.length)(false)
  private var initializedCnt = 0

  //expiration duration setup
  private val expDuration = conf.getLong("cpslab.lsh.plsh.benchmark.expDuration")
  if (expDuration > 0) {
    context.setReceiveTimeout(expDuration milliseconds)
  }

  override def preStart(): Unit = {
    val system = context.system
    import system.dispatcher
    // load the actors
    for (line <- Source.fromFile(inputSource).getLines()) {
      queries += new SparseVector(Vectors.fromString(line))
    }
    //sending window update to all remote workers
    actors.foreach(actor => actor ! WindowUpdate(currentLowerBound, currentUpperBound))
    benchmarkTask = system.scheduler.schedule(0 milliseconds, sendInterval milliseconds, self,
      IOTicket)
  }

  override def postStop(): Unit = {
    //grouped size
    val latencyForEachQuery = readTimeMeasurementResults.groupBy(_._1)
    if (!latencyForEachQuery.isEmpty) {
      val max = latencyForEachQuery.maxBy(_._2.maxBy(_._2))
      val min = latencyForEachQuery.maxBy(_._2.minBy(_._2))
      //get average
      val queryAverages = {
        for ((vectorId, latencies) <- latencyForEachQuery)
          yield latencies.map(_._2).sum * 1.0 / latencies.size
      }
      val avr = queryAverages.sum / queryAverages.toList.size
      println(s"Max: $max, Min: $min, Average: $avr")
    }
  }

  override def receive: Receive = {
    case SimilarityOutput(queryID, bitmap, similarVectors, latency) =>
      latency.foreach(latency => readTimeMeasurementResults += queryID -> latency)
    case CapacityFullNotification(id) =>
      if (id >= currentLowerBound && id <= currentUpperBound) {
        actors.foreach(actor => actor ! WindowUpdate(currentLowerBound + 1, currentUpperBound + 1))
        currentLowerBound += 1
        currentUpperBound += 1
      }
    case WindowUpdateNotification(id) =>
      if (!slidingWindowInitializedFlag(id)) {
        slidingWindowInitializedFlag(id) = true
        initializedCnt += 1
      }
    case IOTicket =>
      if (initializedCnt >= actors.size) {
        actors.zipWithIndex.foreach { case (actor, id) =>
          if (slidingWindowInitializedFlag(id)) actor ! SearchRequest(queries(queryIndex))
        }
        queryIndex += 1
        if (queryIndex > queries.length - 1 && benchmarkTask != null) {
          benchmarkTask.cancel()
        }
      }
    case ReceiveTimeout =>
      context.stop(self)
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
