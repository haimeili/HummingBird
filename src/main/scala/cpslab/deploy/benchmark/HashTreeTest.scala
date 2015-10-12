package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor.{ActorRef, Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.db.{ActorBasedPartitionedHTreeMap, PartitionedHTreeMap}
import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{LSHServer, ShardDatabase, Utils}
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import cpslab.utils.{HashPartitioner, Serializers}

object HashTreeTest {

  case object Ticket

  class MonitorActor(totalCount: Long) extends Actor {

    var earliestStartTime = Long.MaxValue
    var latestEndTime = Long.MinValue

    val receivedActors = new mutable.HashSet[String]

    override def preStart() {
      val system = context.system
      import system.dispatcher
      context.system.scheduler.schedule(0 milliseconds, 30 * 1000 milliseconds, self, Ticket)
    }


    override def receive: Receive = {
      case (startTime: Long, endTime:Long) =>
        val senderPath = sender().path.toString
        if (!receivedActors.contains(senderPath)) {
          earliestStartTime = math.min(earliestStartTime, startTime)
          latestEndTime = math.max(latestEndTime, endTime)
          receivedActors += senderPath
        }
      case Ticket =>
        if (earliestStartTime != Long.MaxValue && latestEndTime != Long.MinValue) {
          println(totalCount * 1.0 / ((latestEndTime - earliestStartTime) / 1000000000))
        }
    }
  }


  def initializeActorBasedHashTree(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.lsh.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new ActorBasedPartitionedHTreeMap[Int, Boolean](
            conf,
            tableId,
            "lsh",
            workingDirRoot + "-" + tableId,
            "partitionedTree-" + tableId,
            new HashPartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            null,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
          newTree
      }
    def initializeIdToVectorMap(conf: Config): PartitionedHTreeMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "Doraemon" =>
          new ActorBasedPartitionedHTreeMap[Int, SparseVector](
            conf,
            tableNum,
            "default",
            workingDirRoot + "-vector",
            "vectorIdToVector",
            new HashPartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            Serializers.vectorSerializer,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
      }
    ActorBasedPartitionedHTreeMap.actorSystem = ActorSystem("AK", conf)
    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
    }
    vectorIdToVector = initializeIdToVectorMap(conf)
  }

  def asyncTestWriteThreadScalability (
    conf: Config, requestNumberPerThread: Int, threadNumber: Int): Unit = {
    //implicit val executorContext = ExecutionContext.fromExecutor(
    //  new ForkJoinPool(threadNumber, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, false))
    initializeActorBasedHashTree(conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.dispatcher

    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    //val replica = conf.getInt("cpslab.lsh.benchmark.replica")
    val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))
    var cnt = 0
    ActorBasedPartitionedHTreeMap.tableNum = tableNum
    def traverseAllFiles(): Unit = {
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        cnt += 1
        if (cnt > cap * threadNumber) {
          return
        }
        val (id, size, indices, values) = Vectors.fromString1(line)
        val vector = new SparseVector(id, size, indices, values)
        vectorIdToVector.put(id, vector)
      }
    }
    ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
      Props(new MonitorActor(cap * threadNumber)),
      name = "monitor")
    traverseAllFiles()
    ActorBasedPartitionedHTreeMap.actorSystem.awaitTermination()

  }

  val finishedWriteThreadCount = new AtomicInteger(0)

  def testWriteThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {
    ShardDatabase.initializeMapDBHashMap(conf)

    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    //for (i <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        val base = 0
        var totalTime = 0L

        private def traverseFile(allFiles: Seq[String]): Unit = {
          var cnt = 0
          for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
            val (id, size, indices, values) = Vectors.fromString1(line)
            val vector = new SparseVector(id, size, indices, values)
            if (cnt >= cap) {
              return
            }
            val s = System.nanoTime()
            vectorIdToVector.put(id, vector)
            for (i <- 0 until tableNum) {
              vectorDatabase(i).put(id, true)
            }
            val e = System.nanoTime()
            totalTime += e - s
            cnt += 1
          }
        }

        override def run(): Unit = {
          val random = new Random(Thread.currentThread().getName.hashCode)
          val allFiles = random.shuffle(Utils.buildFileListUnderDirectory(filePath))
          traverseFile(allFiles)
          /*
          for (i <- 0 until 20) {
            vectorIdToVector.persist(i)
          }
          for (i <- 0 until tableNum; p <- 0 until 20) {
            vectorDatabase(i).persist(p)
          }*/
         // println(cap / (totalTime / 1000000000))
          finishedWriteThreadCount.incrementAndGet()
        }
      })
    //}
  }

  def testReadThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {

    //ShardDatabase.initializeMapDBHashMap(conf)
    //init database by filling vectors
    /*
    ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.cap"),
      conf.getInt("cpslab.lsh.tableNum"))*/

    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {

        var max: Long = Int.MinValue
        var min: Long = Int.MaxValue
        var average: Long = 0

        override def run(): Unit = {
          val startTime = System.nanoTime()
          for (i <- 0 until requestNumberPerThread) {
            val interestVectorId = Random.nextInt(cap)
            for (tableId <- 0 until tableNum) {
              ShardDatabase.vectorDatabase(tableId).getSimilar(interestVectorId)
            }
          }
          println ((System.nanoTime() - startTime) / 1000000000)
        }
      })
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    LSHServer.lshEngine = new LSH(conf)
    val requestPerThread = conf.getInt("cpslab.lsh.benchmark.requestNumberPerThread")
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")
    /*testWriteThreadScalability(conf, requestPerThread, threadNumber)
    while (finishedWriteThreadCount.get() < threadNumber) {
      Thread.sleep(10000)
    }
    testReadThreadScalability(conf, requestNumberPerThread = requestPerThread,
      threadNumber = threadNumber)*/

    if (args(1) == "async") {
      asyncTestWriteThreadScalability(conf, requestPerThread, threadNumber)
    } else {
      testWriteThreadScalability(conf, requestPerThread, threadNumber)
    }

    while (finishedWriteThreadCount.get() < 1) {
        Thread.sleep(10000)
    }
    testReadThreadScalability(conf, requestPerThread, threadNumber)

    //while (finishedWriteThreadCount.get() < threadNumber) {
      //Thread.sleep(10000)
    //}
  }
}
