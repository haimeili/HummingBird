package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, ForkJoinPool}

import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.io.Source
import scala.util.{Success, Failure, Random}

import com.typesafe.config.{Config, ConfigFactory}
import cpslab.db.{ActorBasedPartitionedHTreeMap, PartitionedHTreeMap}
import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{LSHServer, ShardDatabase, Utils}
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import cpslab.utils.{HashPartitioner, Serializers, RangePartitioner}

object HashTreeTest {

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
          new PartitionedHTreeMap[Int, SparseVector](
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
    import ExecutionContext.Implicits.global

    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))
    var cnt = 0
    for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
      cnt += 1
      if (cnt >= cap) {
        return
      }
      val (id, size, indices, values) = Vectors.fromString1(line)
      val vector = new SparseVector(id, size, indices, values)
      Future {
        vectorIdToVector.put(vector.vectorId, vector)
        for (i <- 0 until tableNum) {
          vectorDatabase(i).put(vector.vectorId, true)
        }
      }
    }

    while (true) {
      var stop = true
      for (i <- 0 until tableNum) {
        stop = vectorDatabase(i).size >= cap
      }
      if (stop) {

        return
      }
      Thread.sleep(1000)
    }
  }

  def testWriteThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {
    ShardDatabase.initializeMapDBHashMap(conf)

    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    for (i <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        val base = i
        var totalTime = 0L

        private def traverseFile(allFiles: Seq[String]): Unit = {
          var cnt = 0
          for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
            val (_, size, indices, values) = Vectors.fromString1(line)
            val vector = new SparseVector(cnt + cap * base, size, indices, values)
            if (cnt > cap) {
              return
            }
            val s = System.nanoTime()
            vectorIdToVector.put(cnt + cap * base, vector)
            for (i <- 0 until tableNum) {
              vectorDatabase(i).put(cnt + cap * base, true)
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
          println(cap / (totalTime / 1000000000))
        }
      })
    }
  }

  def testReadThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {

    ShardDatabase.initializeMapDBHashMap(conf)
    //init database by filling vectors
    ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.replica"),
      conf.getInt("cpslab.lsh.benchmark.offset"),
      conf.getInt("cpslab.lsh.benchmark.cap"))

    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          System.out.println("startTime: " + System.nanoTime())
          for (i <- 0 until requestNumberPerThread) {
            val interestVectorId = Random.nextInt(cap)
            for (tableId <- 0 until tableNum) {
              ShardDatabase.vectorDatabase(tableId).getSimilar(interestVectorId)
            }
          }
          System.out.println("endTime: " + System.nanoTime())
        }
      })
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    LSHServer.lshEngine = new LSH(conf)
    val requestPerThread = conf.getInt("cpslab.lsh.benchmark.requestNumberPerThread")
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")
    //testReadThreadScalability(conf, requestNumberPerThread = requestPerThread,
    //  threadNumber = threadNumber)
    //asyncTestWriteThreadScalability(conf, requestPerThread, threadNumber)
    testWriteThreadScalability(conf, requestPerThread, threadNumber)
  }
}
