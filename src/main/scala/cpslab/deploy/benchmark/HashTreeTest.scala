package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.{LinkedBlockingQueue, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.db.{ActorBasedPartitionedHTreeMap, PartitionedHTreeMap}
import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{LSHServer, ShardDatabase, Utils}
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}
import cpslab.lsh.{LSH, LocalitySensitiveHasher}
import cpslab.utils.{LocalitySensitivePartitioner, HashPartitioner, Serializers}

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
          println(s"total number of receivedActors: ${receivedActors.size}")
          println(totalCount * 1.0 / ((latestEndTime - earliestStartTime) / 1000000000))
          println("===SEGMENTS===")
          for (tableID <- ActorBasedPartitionedHTreeMap.histogramOfSegments.indices) {
            println(s"Table $tableID")
            for (partitionId <- ActorBasedPartitionedHTreeMap.histogramOfSegments(tableID).
              indices) {
              println(s"Partition $partitionId")
              val partitionTable =
                ActorBasedPartitionedHTreeMap.histogramOfSegments(tableID)(partitionId)
              for (segmendId <- partitionTable.indices) {
                print(s"$segmendId:${partitionTable(segmendId)}\t")
              }
            }
            println()
          }
          println("===PARTITIONS===")
          for (tableId <- ActorBasedPartitionedHTreeMap.histogramOfPartitions.indices) {
            println(s"Table $tableId")
            for (partitionId <-
                 ActorBasedPartitionedHTreeMap.histogramOfPartitions(tableId).indices) {
              print(s"$partitionId:" +
                s"${ActorBasedPartitionedHTreeMap.histogramOfPartitions(tableId)(partitionId)}\t")
            }
            println()
          }
        }
    }
  }


  def initializeActorBasedHashTree(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.lsh.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val bucketBits = conf.getInt("cpslab.lsh.bucketBits")
    val confForPartitioner = ConfigFactory.parseString(
      s"""
        |cpslab.lsh.vectorDim=32
        |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new ActorBasedPartitionedHTreeMap[Int, Boolean](
            conf,
            tableId,
            "lsh",
            workingDirRoot + "-" + tableId,
            "partitionedTree-" + tableId,
            //new HashPartitioner[Int](numPartitions),
            new LocalitySensitivePartitioner[Int](confForPartitioner, tableId, partitionBits),
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
    PartitionedHTreeMap.BUCKET_LENGTH = bucketBits
    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
    ActorBasedPartitionedHTreeMap.histogramOfPartitions = new Array[Array[Int]](tableNum)
    ActorBasedPartitionedHTreeMap.histogramOfSegments = new Array[Array[Array[Int]]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
      ActorBasedPartitionedHTreeMap.histogramOfSegments(tableId) = new Array[Array[Int]](
        math.pow(2, partitionBits).toInt)
      for (partition <- 0 until math.pow(2, partitionBits).toInt) {
        ActorBasedPartitionedHTreeMap.histogramOfSegments(tableId)(partition) = new Array[Int](
          math.pow(2, 32 - bucketBits).toInt)
      }
      ActorBasedPartitionedHTreeMap.histogramOfPartitions(tableId) = new Array[Int](
        math.pow(2, partitionBits).toInt)
    }
    vectorIdToVector = initializeIdToVectorMap(conf)
  }

  def asyncTestWriteThreadScalability (
    conf: Config, requestNumberPerThread: Int, threadNumber: Int): Unit = {
    //implicit val executorContext = ExecutionContext.fromExecutor(
    // new ForkJoinPool(threadNumber, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, false))
    initializeActorBasedHashTree(conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.dispatcher

    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    //val replica = conf.getInt("cpslab.lsh.benchmark.replica")
    var cnt = 0
    ActorBasedPartitionedHTreeMap.tableNum = tableNum
    def traverseAllFiles(): Unit = {
      val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        cnt += 1
        if (cnt > cap * threadNumber) {
          return
        }
        val (_, size, indices, values) = Vectors.fromString1(line)
        val vector = new SparseVector(cnt, size, indices, values)
        vectorIdToVector.put(cnt, vector)
      }
    }
    ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
      props = Props(new MonitorActor(cap * threadNumber)),
      name = "monitor")
    traverseAllFiles()
    ActorBasedPartitionedHTreeMap.actorSystem.awaitTermination()

  }

  val finishedWriteThreadCount = new AtomicInteger(0)
  val trainingIDs = new ListBuffer[Int]
  val testIDs = new ListBuffer[Int]

  def testWriteThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {
    val bufferOverflow = conf.getInt("cpslab.bufferOverflow")
    PartitionedHTreeMap.BUCKET_OVERFLOW = bufferOverflow


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
          //val decoder = Charset.forName("US-ASCII").newDecoder()
          for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
            val (_, size, indices, values) = Vectors.fromString1(line)
            val squareSum = math.sqrt(values.foldLeft(0.0){
              case (sum, weight) => sum + weight * weight} )
            val vector = new SparseVector(base * cap + cnt, size, indices,
              values.map(_ / squareSum))
            val s = System.nanoTime()
            vectorIdToVector.put(vector.vectorId, vector)
            for (i <- 0 until tableNum) {
              vectorDatabase(i).put(vector.vectorId, true)
            }
            val e = System.nanoTime()
            totalTime += e - s
            cnt += 1
            if (cnt >= cap) {
              return
            }
          }
        }

        override def run(): Unit = {
          val random = new Random(Thread.currentThread().getName.hashCode)
          val allFiles = random.shuffle(Utils.buildFileListUnderDirectory(filePath))
          traverseFile(allFiles)
          println(cap / (totalTime / 1000000000))
          finishedWriteThreadCount.incrementAndGet()
        }
      })
    }
  }

  def testReadThreadScalabilityBTree(conf: Config,
                                     requestNumberPerThread: Int,
                                     threadNumber: Int): Unit = {
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
              ShardDatabase.vectorDatabaseBTree(tableId).get(interestVectorId)
            }
          }
          val duration = System.nanoTime() - startTime
          println(requestNumberPerThread / (duration.toDouble / 1000000000))
          /*println(
            ((System.nanoTime() - startTime) / 1000000000) * 1.0 / requestNumberPerThread + "," +
              max * 1.0 / 1000000000 + "," +
              min * 1.0 / 1000000000)*/
        }
      })
    }
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
            val interestVectorId = Random.nextInt(cap * threadNumber)
            for (tableId <- 0 until tableNum) {
              ShardDatabase.vectorDatabase(tableId).getSimilar(interestVectorId)
            }
          }
          val duration = System.nanoTime() - startTime
          println(requestNumberPerThread / (duration.toDouble / 1000000000))
          /*println(
            ((System.nanoTime() - startTime) / 1000000000) * 1.0 / requestNumberPerThread + "," +
              max * 1.0 / 1000000000 + "," +
              min * 1.0 / 1000000000)*/
        }
      })
    }
  }

  def testWriteThreadScalabilityOnheap(
      conf: Config,
      requestNumberPerThread: Int,
      threadNumber: Int): Unit = {
    ShardDatabase.initializeMapDBHashMapOnHeap(conf)

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
            val vector = new SparseVector(cnt + base * cap, size, indices, values)
            if (cnt >= cap) {
              return
            }
            val s = System.nanoTime()
            vectorIdToVectorOnheap.put(cnt + base * cap, vector)
            for (i <- 0 until tableNum) {
              vectorDatabaseOnheap(i).put(cnt + base * cap, true)
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
          finishedWriteThreadCount.incrementAndGet()
        }
      })
    }
  }

  def testReadThreadScalabilityOnheap(
      conf: Config,
      requestNumberPerThread: Int,
      threadNumber: Int): Unit = {
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {

        var max: Long = Int.MinValue
        var min: Long = Int.MaxValue
        var average: Long = 0

        override def run(): Unit = {
          startTime = System.nanoTime()
          for (i <- 0 until requestNumberPerThread) {
            val interestVectorId = Random.nextInt(cap)
            val localStart = System.nanoTime()
            for (tableId <- 0 until tableNum) {
              ShardDatabase.vectorDatabaseOnheap(tableId).getSimilar(interestVectorId)
            }
            val localEnd = System.nanoTime()
            val latency = localEnd - localStart
            max = math.max(latency, max)
            min = math.min(latency, min)
          }
          println(
            ((System.nanoTime() - startTime) / 1000000000) * 1.0 / requestNumberPerThread + "," +
              max * 1.0 / 1000000000 + "," +
              min * 1.0 / 1000000000)
        }
      })
    }
  }

  def testWriteThreadScalabilityWithBTree(conf: Config,
                                          requestNumberPerThread: Int,
                                          threadNumber: Int): Unit = {
    ShardDatabase.initializeBTree(conf)

    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    //initialize lsh engine
    val lshEngines = for (i <- 0 until tableNum)
      yield new LocalitySensitiveHasher(LSHServer.getLSHEngine, i)
    for (i <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        val base = i
        var totalTime = 0L

        private def traverseFile(allFiles: Seq[String]): Unit = {
          var cnt = 0
          for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
            val (_, size, indices, values) = Vectors.fromString1(line)
            val vector = new SparseVector(cnt + base * cap, size, indices, values)
            if (cnt >= cap) {
              return
            }
            val s = System.nanoTime()
            vectorIdToVectorBTree.put(cnt + base * cap, vector)
            for (i <- 0 until tableNum) {
              val hashValue = lshEngines(i).hash(vector, Serializers.VectorSerializer)
              vectorDatabaseBTree(i).put(hashValue, vector.vectorId)
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
          println(cap / (totalTime / 1000000000))
          finishedWriteThreadCount.incrementAndGet()
        }
      })
    }
  }


  def testAccuracy(conf: Config): Unit = {
    import scala.collection.JavaConversions._
    var ratio = 0.0
    var totalCnt = 50
    for (testCnt <- 0 until totalCnt) {
      val order = Random.nextInt(testIDs.size)
      val queryVector = vectorIdToVector.get(testIDs(order))
      println("query vector ID:" + queryVector.vectorId)
      val tableNum = conf.getInt("cpslab.lsh.tableNum")
      val mostK = conf.getInt("cpslab.lsh.k")
      val kNN = new mutable.HashSet[Int]
      for (i <- 0 until tableNum) {
        val r = vectorDatabase(i).getSimilar(queryVector.vectorId)
        for (k <- r if k != queryVector.vectorId) {
          kNN += k
        }
      }
      //step 1: calculate the distance of the fetched objects
      val distances = new ListBuffer[(Int, Double)]
      for (vectorId <- kNN) {
        val vector = vectorIdToVector.get(vectorId)
        distances += vectorId -> SimilarityCalculator.fastCalculateSimilarity(queryVector, vector)
      }
      val sortedDistances = distances.sortWith { case (d1, d2) => d1._2 > d2._2 }.take(mostK)
      println(sortedDistances.toList)
      //step 2: calculate the distance of the ground truth
      val groundTruth = new ListBuffer[(Int, Double)]
      val itr = trainingIDs.iterator
      while (itr.hasNext) {
        val vId = itr.next()
        val vector = vectorIdToVector.get(vId)
        if (vector.vectorId != queryVector.vectorId) {
          groundTruth +=
            vector.vectorId -> SimilarityCalculator.fastCalculateSimilarity(queryVector, vector)
        }
      }
      val sortedGroundTruth = groundTruth.sortWith {
        case (d1, d2) => d1._2 > d2._2 }.take(mostK)
      println(sortedGroundTruth.toList)
      ratio += {
        var sum = 0.0
        for (i <- sortedDistances.indices) {
          sum += sortedDistances(i)._2 /sortedGroundTruth(i)._2
          //sortedGroundTruth(i)._2 / sortedDistances(i)._2
        }
        //sum += math.max(mostK - sortedDistances.length, 0)
        if (sortedDistances.size > 0) {
          sum / sortedDistances.length
        } else {
          //totalCnt -= 1
          0
        }
      }
    }
    println(ratio/totalCnt)
  }

  def loadAccuracyTestFiles(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    def loadFiles(files: Seq[String], updateExistingID: ListBuffer[Int]): Unit = {
      for (file <- files; line <- Source.fromFile(file).getLines()) {
        val (id, size, indices, values) = Vectors.fromString(line)
        updateExistingID += id
        val squareSum = math.sqrt(values.foldLeft(0.0) {
          case (sum, weight) => sum + weight * weight })
        val vector = new SparseVector(id, size, indices,
          values.map(_ / squareSum))
        vectorIdToVector.put(id, vector)
        for (i <- 0 until tableNum) {
          vectorDatabase(i).put(id, true)
        }
      }
    }

    val bufferOverflow = conf.getInt("cpslab.bufferOverflow")
    PartitionedHTreeMap.BUCKET_OVERFLOW = bufferOverflow

    ShardDatabase.initializeMapDBHashMap(conf)

    val trainingPath = conf.getString("cpslab.lsh.trainingPath")
    val testPath = conf.getString("cpslab.lsh.testPath")
    val allTrainingFiles = Utils.buildFileListUnderDirectory(trainingPath)
    val allTestFiles = Utils.buildFileListUnderDirectory(testPath)
    loadFiles(allTrainingFiles, trainingIDs)
    loadFiles(allTestFiles, testIDs)
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    LSHServer.lshEngine = new LSH(conf)
    val requestPerThread = conf.getInt("cpslab.lsh.benchmark.requestNumberPerThread")
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")


    loadAccuracyTestFiles(conf)

    testAccuracy(conf)

    //initializeActorBasedHashTree(conf)

    /*ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.cap"),
      conf.getInt("cpslab.lsh.tableNum"))*/


    /*
    testWriteThreadScalability(conf, requestPerThread, threadNumber)

    while (finishedWriteThreadCount.get() < threadNumber) {
      Thread.sleep(10000)
    }
    println("======read performance======")
    testReadThreadScalability(conf, requestPerThread, threadNumber)*/

/*

    if (args(1) == "async") {
      asyncTestWriteThreadScalability(conf, requestPerThread, threadNumber)
    } else {
      testWriteThreadScalability(conf, requestPerThread, threadNumber)
    }*/
  }
}
