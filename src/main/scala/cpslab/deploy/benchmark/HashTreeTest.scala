package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{LinkedBlockingQueue, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.StringBuilder
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor.{Cancellable, Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.db._
import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{LSHServer, ShardDatabase, Utils}
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}
import cpslab.lsh.{LSH, LocalitySensitiveHasher}
import cpslab.utils.{LocalitySensitivePartitioner, HashPartitioner, Serializers}

object HashTreeTest {

  case object Ticket
  case object TicketForRead

  class MonitorActor(conf: Config, requestNumPerThread: Int, threadNum: Int,
                     totalWriteCount: Int, totalReadCount: Int, ifRunReadTest: Boolean)
    extends Actor {

    val receivedActors = new mutable.HashMap[String, (Int, Int)]

    var earliestStartTime = Long.MaxValue
    var latestEndTime = Long.MinValue

    var earliestReadStartTime = Long.MaxValue
    var latestReadEndTime = Long.MinValue

    var ticketScheduler: Cancellable = null

    var readStarted = false

    override def preStart() {
      val system = context.system
      import system.dispatcher
      ticketScheduler =
        context.system.scheduler.schedule(0 milliseconds, 30 * 1000 milliseconds, self, Ticket)
    }

    var totalMainTableMsgCnt = new mutable.HashMap[String, Int]()
    var totalLSHTableMsgCnt = new mutable.HashMap[String, Int]()

    private def reportReadPerf(): Unit = {
      if (earliestReadStartTime != Long.MaxValue && latestReadEndTime != Long.MinValue) {
        println(s"total number of receivedActors: ${receivedActors.size}")
        // println(s"total throughput: $totalThroughput")
        println(s"total Throughput: ${
          totalReadCount * 1.0 /
            ((latestReadEndTime - earliestReadStartTime) / 1000000000)
        }")
      }
    }

    private def report(): (Int, Int) = {
      if (earliestStartTime != Long.MaxValue && latestEndTime != Long.MinValue) {
        println(s"total number of receivedActors: ${receivedActors.size}")
        // println(s"total throughput: $totalThroughput")
        println(s"total Throughput: ${
          totalWriteCount * 1.0 /
            ((latestEndTime - earliestStartTime) / 1000000000)
        }")
        val mainTableMsgCount = {
          var r = 0
          for ((actorName, v) <- totalMainTableMsgCnt) {
            r += v
          }
          r
        }
        val lshTableMsgCount = {
          var r = 0
          for ((actorName, v) <- totalLSHTableMsgCnt) {
            r += v
          }
          r
        }
        println(s"total message number: $mainTableMsgCount, $lshTableMsgCount")
        (mainTableMsgCount, lshTableMsgCount)
      } else {
        (0, 0)
      }
    }

    private def processingTicket(): Unit = {
      val (mainMsgNum, lshTableMsgNum) = report()
      if (ifRunReadTest && mainMsgNum >= totalWriteCount &&
        lshTableMsgNum == 10 * mainMsgNum && !readStarted) {
        println("===Read Performance ===")
        val system = context.system
        import system.dispatcher
        earliestStartTime = Long.MaxValue
        latestEndTime = Long.MinValue
        totalLSHTableMsgCnt.clear()
        totalMainTableMsgCnt.clear()
        ticketScheduler.cancel()
        ticketScheduler = context.system.scheduler.schedule(0 milliseconds,
          30 * 1000 milliseconds, self, TicketForRead)
        readStarted = true
        asyncTestReadThreadScalability(conf, requestNumPerThread)
      }
    }

    override def receive: Receive = {
      case Tuple4(startTime: Long, endTime: Long, mainTableCnt: Int, lshTableCnt: Int) =>
        earliestStartTime = math.min(earliestStartTime, startTime)
        latestEndTime = math.max(latestEndTime, endTime)
        val senderPath = sender().path.toString
        if (!receivedActors.contains(senderPath) ||
          (receivedActors(senderPath)._1 != mainTableCnt ||
            receivedActors(senderPath)._2 != lshTableCnt)) {
          receivedActors += (senderPath -> Tuple2(mainTableCnt, lshTableCnt))
          totalMainTableMsgCnt += (senderPath -> mainTableCnt)
          totalLSHTableMsgCnt += (senderPath -> lshTableCnt)
        }
      case Tuple3(startTime: Long, endTime: Long, msgCnt: Int) =>
        earliestReadStartTime = math.min(earliestReadStartTime, startTime)
        latestReadEndTime = math.max(latestReadEndTime, endTime)
        val senderPath = sender().path.toString
        if (!receivedActors.contains(senderPath) ||
          receivedActors(senderPath)._1 != msgCnt) {
          println(s"received actor $senderPath with $msgCnt messages")
          receivedActors += senderPath -> Tuple2(msgCnt, 0)
        }
      case Ticket =>
        processingTicket()
      case TicketForRead =>
        reportReadPerf()
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
    val dirNodeSize = conf.getInt("cpslab.lsh.htree.dirNodeSize")
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
    PartitionedHTreeMap.updateBucketLength(bucketBits)
    PartitionedHTreeMap.updateDirectoryNodeSize(dirNodeSize)
    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
    ActorBasedPartitionedHTreeMap.histogramOfPartitions = new Array[Array[Int]](tableNum)
    ActorBasedPartitionedHTreeMap.histogramOfSegments = new Array[Array[Array[Int]]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
      vectorDatabase(tableId).initStructureLocks()
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
    vectorIdToVector.initStructureLocks()
  }

  def asyncTestWriteThreadScalability (
    conf: Config, threadNumber: Int): Unit = {
    val actorNumPerPartition = conf.getInt("cpslab.lsh.benchmark.actorNum")
    ActorBasedPartitionedHTreeMap.writerActorsNumPerPartition = actorNumPerPartition
    val parallelLSHCalculation = conf.getBoolean("cpslab.lsh.benchmark.parallelLSH")
    ActorBasedPartitionedHTreeMap.parallelLSHComputation = parallelLSHCalculation
    val bufferSize = conf.getInt("cpslab.lsh.benchmark.bufferSize")
    ActorBasedPartitionedHTreeMap.bufferSize = bufferSize
    ActorBasedPartitionedHTreeMap.totalFeedingThreads = threadNumber


    initializeActorBasedHashTree(conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.dispatcher

    val cap = conf.getInt("cpslab.lsh.benchmark.asyncCap")
    val readCap = conf.getInt("cpslab.lsh.benchmark.asyncReadCap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val replica = conf.getInt("cpslab.lsh.benchmark.replica")
    val ifRunRead = conf.getBoolean("cpslab.lsh.benchmark.ifRunReadTest")
    ActorBasedPartitionedHTreeMap.tableNum = tableNum
    def traverseAllFiles(): Unit = {
      for (i <- 0 until threadNumber) {
        new Thread(new Runnable {
          val base = i
          override def run(): Unit = {
            var cnt = 0
            val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))
            for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
              if (cnt > cap) {
                println(s"all requests sent in thread ${Thread.currentThread().getName}")
                ActorBasedPartitionedHTreeMap.stoppedFeedingThreads.incrementAndGet()
                return
              }
              val (_, size, indices, values) = Vectors.fromString1(line)
              // for (i <- 0 until replica) {
              val vector = new SparseVector(cnt + cap * base, size, indices, values)
              vectorIdToVector.put(cnt + cap * base, vector)
              cnt += 1
              // }
            }
          }
        }, s"thread-$i").start()
      }
    }
    ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
      props = Props(new MonitorActor(conf, cap, threadNumber, cap * threadNumber,
        readCap * threadNumber, ifRunRead)),
      name = "monitor")
    traverseAllFiles()
    ActorBasedPartitionedHTreeMap.actorSystem.awaitTermination()
  }

  val finishedWriteThreadCount = new AtomicInteger(0)
  val trainingIDs = new ListBuffer[Int]
  val testIDs = new ListBuffer[Int]

  def testWriteThreadScalability(
    conf: Config,
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
            val (vectorId, size, indices, values) = Vectors.fromString1(line)
            val squareSum = math.sqrt(values.foldLeft(0.0){
              case (sum, weight) => sum + weight * weight} )
            //base * cap + cnt
            val vector = new SparseVector(vectorId, size, indices, values.map(_ / squareSum))
            val s = System.currentTimeMillis()
            val h = ValueAndHash(vector, 0)
            vectorIdToVector.put(vector.vectorId, vector)
            for (i <- 0 until tableNum) {
              val h = KeyAndHash(0, 0, 0)
              vectorDatabase(i).put(vector.vectorId, true)
            }
            val e = System.currentTimeMillis()
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
          println(cap / (totalTime / 1000))
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

  def asyncTestReadThreadScalability(conf: Config,
                                     requestNumberPerThread: Int): Unit = {
    val actorNumPerPartition = conf.getInt("cpslab.lsh.benchmark.readerActorNum")
    ActorBasedPartitionedHTreeMap.readerActorsNumPerPartition = actorNumPerPartition
    val parallelLSHCalculation = conf.getBoolean("cpslab.lsh.benchmark.parallelLSH")
    ActorBasedPartitionedHTreeMap.parallelLSHComputation = parallelLSHCalculation
    val bufferSize = conf.getInt("cpslab.lsh.benchmark.bufferSize")
    ActorBasedPartitionedHTreeMap.bufferSize = bufferSize
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.readingThreadNum")
    ActorBasedPartitionedHTreeMap.totalReadingThreads = threadNumber

    val readCap = conf.getInt("cpslab.lsh.benchmark.asyncReadCap")
    val cap = conf.getInt("cpslab.lsh.benchmark.asyncCap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val threadPool = Executors.newFixedThreadPool(threadNumber)

    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId).asInstanceOf[ActorBasedPartitionedHTreeMap[Int,  Boolean]].
        initReaderActors()
    }

    val buffer = new mutable.HashMap[String, ListBuffer[(Int, Int)]]
    val bufferLocks = new mutable.HashMap[String, ReentrantReadWriteLock]

    for (partitionId <- 0 until vectorDatabase(0).partitioner.numPartitions;
         actorId <- 0 until ActorBasedPartitionedHTreeMap.readerActorsNumPerPartition) {
      val actorIndex = s"$partitionId-$actorId"
      buffer.put(actorIndex, new ListBuffer[(Int, Int)])
      bufferLocks.put(actorIndex, new ReentrantReadWriteLock())
    }

    def dumpBuffer(): Unit = {
      for ((actorIndex, bufferLock) <- bufferLocks) {
        val bufferWriteLock = bufferLock.writeLock()
        try {
          bufferWriteLock.lock()
          val Array(partitionId, actorId) = actorIndex.split("-")
          val actor = ActorBasedPartitionedHTreeMap.readerActors(partitionId.toInt)(actorId.toInt)
          if (buffer(actorIndex).nonEmpty) {
            actor ! BatchQueryRequest(buffer(actorIndex).toList)
            buffer(actorIndex) = new ListBuffer[(Int, Int)]
          }
        } finally {
          bufferWriteLock.unlock()
        }
      }
    }

    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          for (i <- 0 until readCap) {
            val interestVectorId = Random.nextInt(cap * threadNumber)
            for (tableId <- 0 until tableNum) {
              val table = ShardDatabase.vectorDatabase(tableId).
                asInstanceOf[ActorBasedPartitionedHTreeMap[Int, Boolean]]
              val hash = table.hash(interestVectorId)
              val partitionId = table.asInstanceOf[ActorBasedPartitionedHTreeMap[Int, Boolean]].
                partitioner.getPartition(hash)
              val segId = hash >>> PartitionedHTreeMap.BUCKET_LENGTH
              val actorId = math.abs(s"$tableId-$segId".hashCode) %
                ActorBasedPartitionedHTreeMap.readerActorsNumPerPartition
              if (bufferSize > 0) {
                val actorIndex = s"$partitionId-$actorId"
                val bufferLock = bufferLocks(actorIndex).writeLock()
                try {
                  bufferLock.lock()
                  buffer(actorIndex) += Tuple2(tableId, interestVectorId)
                  if (buffer(actorIndex).length >= bufferSize) {
                    val actor = ActorBasedPartitionedHTreeMap.readerActors(partitionId)(actorId)
                    actor ! BatchQueryRequest(buffer(actorIndex).toList)
                    buffer(actorIndex) = new ListBuffer[(Int, Int)]
                  }
                } catch {
                  case e: Exception =>
                    e.printStackTrace()
                } finally {
                  bufferLock.unlock()
                }
              } else {
                ActorBasedPartitionedHTreeMap.readerActors(partitionId)(actorId) !
                  QueryRequest(tableId, interestVectorId)
              }
            }
          }
          dumpBuffer()
          println(s"thread $t finished sending read requests")
          ActorBasedPartitionedHTreeMap.stoppedReadingThreads.getAndIncrement()
        }
      })
    }
  }


  def testReadThreadScalability(
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
              println("all requests sent")
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

  private def readSimilarVectorId(queryVector: SparseVector, tableNum: Int): HashSet[Int] = {
    import scala.collection.JavaConversions._
    var kNN = new HashSet[Int]
    for (i <- 0 until tableNum) {
      val r = vectorDatabase(i).getSimilar(queryVector.vectorId)
      for (k <- r if k != queryVector.vectorId) {
        kNN = kNN + k
      }
    }
    kNN
  }

  private def getkNN(distances: ListBuffer[(Int, Double)], mostK: Int):
    (Double, ListBuffer[(Int, Double)], Boolean) = {
    val kNN = distances.sortWith { case (d1, d2) => d1._2 > d2._2 }.take(mostK)
    var ifOverHit = false
    val efficiency = {
      if (distances.length >= mostK) {
        ifOverHit = true
        distances.length / mostK
      } else {
        0.0
      }
    }
    (efficiency, kNN, ifOverHit)
  }

  def testAccuracy(conf: Config): Unit = {
    import scala.collection.JavaConversions._
    val ratiosInstances = new ListBuffer[Double]
    val effSumInstances = new ListBuffer[Double]
    val experimentalInstances = conf.getInt("cpslab.expInstance")
    val overHitInstances = new Array[Int](experimentalInstances)
    for (exp <- 0 until experimentalInstances) {
      var ratio = 0.0
      val totalCnt = 50
      var efficiencySum = new ListBuffer[Double]
      val tableNum = conf.getInt("cpslab.lsh.tableNum")
      for (testCnt <- 0 until totalCnt) {
        val order = Random.nextInt(testIDs.size)
        val queryVector = vectorIdToVector.get(testIDs(order))
        println("query vector ID:" + queryVector.vectorId)
        val mostK = conf.getInt("cpslab.lsh.k")

        val startTime = System.nanoTime()
        val kNN = readSimilarVectorId(queryVector, tableNum)
        //step 1: calculate the distance of the fetched objects
        val distances = new ListBuffer[(Int, Double)]
        for (vectorId <- kNN) {
          val vector = vectorIdToVector.get(vectorId)
          distances += vectorId -> SimilarityCalculator.fastCalculateSimilarity(queryVector, vector)
        }
        val (efficiency, sortedDistances, ifOverfit) = getkNN(distances, mostK)
        if (efficiency > 0.0) {
          efficiencySum += efficiency
          assert(ifOverfit)
          overHitInstances(exp) += 1
        }
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
          case (d1, d2) => d1._2 > d2._2
        }.take(mostK)
        println(sortedGroundTruth.toList)
        ratio += {
          var sum = 0.0
          for (i <- sortedGroundTruth.indices) {
            if (sortedDistances.length < i + 1) {
              sum += math.acos(0) / math.acos(sortedGroundTruth(i)._2)
            } else {
              sum += math.acos(sortedDistances(i)._2) / math.acos(sortedGroundTruth(i)._2)
            }
          }
          sum / mostK
        }
      }
      //println(ratio / totalCnt)
      //println("efficiency:" + efficiencySum.sum)
      ratiosInstances += ratio / totalCnt
      effSumInstances += efficiencySum.sum
      overHitInstances
    }
    assert(ratiosInstances.length == experimentalInstances)
    assert(effSumInstances.length == experimentalInstances)
    val ratioOutputStr = new StringBuilder()
    val effSumOutputStr = new StringBuilder()
    for (i <- 0 until experimentalInstances) {
      ratioOutputStr.append(ratiosInstances(i).toString)
      ratioOutputStr.append("\t")
      effSumOutputStr.append(effSumInstances(i).toString)
      effSumOutputStr.append("\t")
    }
    println("ratios:" + ratioOutputStr.toString())
    println("efficiency:" + effSumOutputStr.toString())
    println("hitNum:" + overHitInstances.toList.toString())
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
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")

/*
    loadAccuracyTestFiles(conf)

    testAccuracy(conf)*/

    //initializeActorBasedHashTree(conf)

    /*ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.cap"),
      conf.getInt("cpslab.lsh.tableNum"))*/

    if (args(1) == "async") {
      asyncTestWriteThreadScalability(conf, threadNumber)
    } else {
      val requestPerThread = conf.getInt("cpslab.lsh.benchmark.syncReadCap")
      testWriteThreadScalability(conf, threadNumber)
      val ifRunRead = conf.getBoolean("cpslab.lsh.benchmark.ifRunReadTest")
      while (finishedWriteThreadCount.get() < threadNumber) {
        Thread.sleep(10000)
      }
      if (ifRunRead) {
        println("======read performance======")
        testReadThreadScalability(conf, requestPerThread, threadNumber)
      }
    }


    //ActorBasedPartitionedHTreeMap.shareActor = args(2).toBoolean

    // write performance
    /*
    if (args(1) == "async") {
      asyncTestWriteThreadScalability(conf, threadNumber)
    } else {
      testWriteThreadScalability(conf, threadNumber)
    }*/
  }
}
