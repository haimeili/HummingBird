package cpslab.deploy.benchmark

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy._
import cpslab.lsh.vector.{SparseVector, Vectors}

private[benchmark] class BenchmarkClientActor(conf: Config) extends Actor {

  private val inputSource = conf.getString("cpslab.lsh.benchmark.inputSource")
  private val sendInterval = conf.getLong("cpslab.lsh.benchmark.messageInterval")

  private var benchmarkTask: Cancellable = null
  private val queries: ListBuffer[SparseVector] = new ListBuffer[SparseVector]

  //worker list
  private val workersList = conf.getStringList("cpslab.lsh.shardingRouter")
  private val actors = {
    for (routerAddress <- workersList)
      yield context.actorSelection(routerAddress)
  }

  //expiration duration setup
  private val expDuration = conf.getLong("cpslab.lsh.benchmark.expDuration")
  if (expDuration > 0) {
    context.setReceiveTimeout(expDuration milliseconds)
  }

  // vectorID -> Long
  private val startTime = new mutable.HashMap[Int, Long]
  private val endTime = new mutable.HashMap[Int, Long]

  override def preStart(): Unit = {
    val system = context.system
    import system.dispatcher
    // load the actors
    val filePaths = Utils.buildFileListUnderDirectory(inputSource)
    for (inputFile <- filePaths; line <- Source.fromFile(inputFile).getLines()) {
      queries += new SparseVector(Vectors.fromString1(line))
    }
    benchmarkTask = system.scheduler.schedule(0 milliseconds, sendInterval milliseconds, self,
      IOTicket)
  }

  override def postStop(): Unit = {
    //grouped size
    actors.foreach(actor => actor ! BenchmarkEnd)
    val result = new mutable.HashMap[Int, Long]
    endTime.synchronized {
      startTime.synchronized {
        for ((vectorId, endMoment) <- endTime if startTime.contains(vectorId)) {
          result += vectorId -> (endTime(vectorId) - startTime(vectorId))
        }
      }
    }
    if (result.nonEmpty) {
      val max = result.maxBy(_._2)
      val min = result.minBy(_._2)
      val average = result.map(_._2).sum * 1.0 / result.size
      println(s"max $max, min: $min, average: $average")
    }
  }

  override def receive: Receive = {
    case SimilarityOutput(queryID, bitmap, similarVectors, latency) =>
      //TODO: assume zero latency
    case IOTicket =>
      for (query <- queries) {
        startTime += query.vectorId -> System.currentTimeMillis()
        actors(Random.nextInt(actors.length)) ! SearchRequest(query)
      }
      //TODO: support multiple rounds of benchmarking
      if(benchmarkTask != null) {
        benchmarkTask.cancel()
      }
    case ReceiveTimeout =>
      context.stop(self)
    case BenchmarkEnd =>

  }
}

private[benchmark] object BenchmarkClientActor {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: program conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(0)))
    val actorSystem = ActorSystem("LSH", conf)
    actorSystem.actorOf(Props(new BenchmarkClientActor(conf)), name = "benchmarkClient")
  }
}
