package cpslab.deploy

import java.util.concurrent._

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.Config
import cpslab.db.PartitionedHTreeMap
import cpslab.deploy.benchmark.DataSetLoader
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector
import cpslab.utils.{HashPartitioner, RangePartitioner, Serializers}

private[cpslab] object ShardDatabase extends DataSetLoader {

  var partitionedHTreeCount = 0

  var actors: Seq[ActorRef] = null
  @volatile var startTime = -1L
  @volatile var endTime = -1L

  case object Report

  class MonitorActor extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    override def receive: Receive = {
      case ReceiveTimeout =>
        println("Finished building table: " + (endTime - startTime) + " milliseconds")
        println("Monitor Actor Stopped")
      case Report =>
    }
  }

  class InitializeWorker(parallelism: Int, lsh: LSH) extends Actor {

    context.setReceiveTimeout(30000 milliseconds)

    private val monitor = context.actorSelection("/user/monitor")
    private var hasSentReport = false

    override def receive: Receive = {
      case sv: SparseVector =>
        if (hasSentReport) {
          monitor ! Report
          hasSentReport = false
        }
        if (startTime == -1L) {
          startTime = System.currentTimeMillis()
        }
        for (i <- vectorDatabase.indices) {
          vectorDatabase(i).put(sv.vectorId, true)
        }
        val endMoment = System.currentTimeMillis()
        if (endMoment > endTime) {
          endTime = endMoment
        }
      case ReceiveTimeout =>
        if (!hasSentReport) {
          monitor ! Report
          hasSentReport = true
        }
    }
  }

  def initializeMapDBHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.lsh.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    def initializeVectorDatabase(): ConcurrentMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new PartitionedHTreeMap[Int, Boolean](
            partitionedHTreeCount,
            "lsh",
            workingDirRoot + "-" + partitionedHTreeCount,
            "partitionedTree-" + partitionedHTreeCount,
            new RangePartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            null,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
          partitionedHTreeCount += 1
          newTree
      }
    def initializeIdToVectorMap(): ConcurrentMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "Doraemon" =>
          new PartitionedHTreeMap(
            0,
            "default",
            workingDirRoot + "-vector-" + partitionedHTreeCount,
            "partitionedTree-" + partitionedHTreeCount,
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
    vectorDatabase = Array.fill(tableNum)(initializeVectorDatabase())
    vectorIdToVector = initializeIdToVectorMap()
  }

  /**
   * initialize the database by reading raw vector data from file system
   * @param filePath the root path of the data directory
   * @param parallelism the number of actors writing data
   */
  def initVectorDatabaseFromFS(
      lsh: LSH,
      actorSystem: ActorSystem,
      filePath: String,
      parallelism: Int,
      tableNum: Int,
      replica: Int,
      offset: Int,
      cap: Int): Unit = {
    actors = {
      for (i <- 0 until parallelism)
        yield actorSystem.actorOf(Props(new InitializeWorker(parallelism, lsh)))
    }
    initVectorDatabaseFromFS(filePath, replica, offset, cap)
    // start monitor actor
    actorSystem.actorOf(Props(new MonitorActor), name = "monitor")
    //start writing rate monitor thread
    new Thread(
      new Runnable {
        override def run(): Unit = {
          var lastAmount = 0L
          var lastTime = 0L
          while (true) {
            var totalCnt = 0
            for (i <- vectorDatabase.indices) {
              val entrySetItr = vectorDatabase(i).entrySet().iterator()
              while (entrySetItr.hasNext) {
                val a = entrySetItr.next()
                totalCnt += a.getValue.asInstanceOf[ConcurrentLinkedQueue[Int]].size()
              }
            }
            val currentTime = System.currentTimeMillis()
            println(s"Writing Rate ${(totalCnt - lastAmount) * 1.0 /
              ((currentTime - lastAmount) * 1000)}")
            lastAmount = totalCnt
            lastTime = currentTime
            Thread.sleep(1000)
          }
        }
      }
    ).start()
    val itr = vectorIdToVector.values().iterator()
    while (itr.hasNext) {
      val vector = itr.next()
      actors(vector.vectorId % parallelism) ! vector
    }
  }

  private[deploy] var vectorDatabase: Array[ConcurrentMap[Int, Boolean]] = null
  private[deploy] var vectorIdToVector: ConcurrentMap[Int, SparseVector] = null

  def vectorSimilarityDB = vectorDatabase
  def vectorMainDB = vectorIdToVector
}
