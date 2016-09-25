package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ExecutorService, LinkedBlockingQueue, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.StringBuilder
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.{Success, Failure, Random}

import akka.actor.{Cancellable, Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.db._
import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{BTreeDatabase, LSHServer, ShardDatabase, Utils}
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}
import cpslab.lsh.{LSH, LocalitySensitiveHasher}
import cpslab.utils.{LocalitySensitivePartitioner, HashPartitioner, Serializers}

object HashTreeTest {

  //initialize lsh engine
  var lshEngines: Array[LocalitySensitiveHasher] = null
  var lshPartitioners: Array[LocalitySensitivePartitioner[Int]] = null
  var htreeDebug = false
  var usePersistSegment = false
  var persistWorkingDir = ""

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
        var sum = 0
        for (x <- receivedActors) {
          sum += x._2._1
        }
        println(s"total received message: $sum")
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
        println(s"conflict count:" +
          s" ${vectorDatabase(0).asInstanceOf[ActorBasedPartitionedHTreeMap[Int, Boolean]].
            redistributionCount}" + " " +
        s"${vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[Int, SparseVector]].
          redistributionCount}")
        (mainTableMsgCount, lshTableMsgCount)
      } else {
        (0, 0)
      }
    }

    private def processingTicket(): Unit = {
      val (mainMsgNum, lshTableMsgNum) = report()
      if (mainMsgNum >= totalWriteCount) {
        var foundNull = false
        for (i <- 0 until totalWriteCount) {
          if (vectorIdToVector.get(i) == null) {
            println(s"$i is not found")
            foundNull = true
          }
        }
        if (!foundNull) {
          println("ALL VECTOR WRITTEN SUCCESSFULLY")
        }
      }
      if (ifRunReadTest && mainMsgNum >= totalWriteCount &&
        lshTableMsgNum >= conf.getInt("cpslab.lsh.tableNum") * mainMsgNum && !readStarted) {
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

    private def processingReadPerformanceReport(startTime: Long,
                                                endTime: Long,
                                                msgCnt: Int,
                                                batchMsgCnt: Int): Unit = {
      earliestReadStartTime = math.min(earliestReadStartTime, startTime)
      latestReadEndTime = math.max(latestReadEndTime, endTime)
      val senderPath = sender().path.toString
      if (!receivedActors.contains(senderPath) ||
        (receivedActors(senderPath)._1 != msgCnt || receivedActors(senderPath)._2 != batchMsgCnt)
      ) {
        println(s"received $senderPath with $msgCnt messages, $batchMsgCnt batch messages, " +
          s"start time $startTime, end time $endTime, " +
          s"duration ${(endTime - startTime) * 1.0 / 1000000000} s")
        receivedActors += senderPath -> Tuple2(msgCnt, batchMsgCnt)
      }
    }

    override def receive: Receive = {
      case Tuple6(startTime: Long, endTime: Long, mainTableCnt: Int, lshTableCnt: Int,
         batchMainMsg: Int, batchLSHMsg: Int) =>
        if (mainTableCnt != 0 || lshTableCnt != 0) {
          earliestStartTime = math.min(earliestStartTime, startTime)
          latestEndTime = math.max(latestEndTime, endTime)
          val senderPath = sender().path.toString
          if (!receivedActors.contains(senderPath) ||
            (receivedActors(senderPath)._1 != mainTableCnt ||
              receivedActors(senderPath)._2 != lshTableCnt)) {
            println(s"received report from $senderPath with $mainTableCnt main table messages, " +
              s"$lshTableCnt lsh table messages, $batchMainMsg batch main table messages," +
              s" $batchLSHMsg batch lsh table messages")
            receivedActors += (senderPath -> Tuple2(mainTableCnt, lshTableCnt))
            totalMainTableMsgCnt += (senderPath -> mainTableCnt)
            totalLSHTableMsgCnt += (senderPath -> lshTableCnt)
          }
        }
      case ReadPerformanceReport(startTime: Long, endTime: Long, msgCnt: Int, batchMsgCnt: Int) =>
        processingReadPerformanceReport(startTime, endTime, msgCnt, batchMsgCnt)
      case Ticket =>
        processingTicket()
      case TicketForRead =>
        reportReadPerf()
    }
  }


  def asyncTestWriteThreadScalability (
    conf: Config, threadNumber: Int): Unit = {
    val actorNumPerPartition = conf.getInt("cpslab.lsh.benchmark.actorNum")
    ActorBasedPartitionedHTreeMap.writerActorsNumPerPartition = actorNumPerPartition
    val bufferSize = conf.getInt("cpslab.lsh.benchmark.bufferSize")
    ActorBasedPartitionedHTreeMap.bufferSize = bufferSize
    val lshBufferSize = conf.getInt("cpslab.lsh.benchmark.lshBufferSize")
    ActorBasedPartitionedHTreeMap.lshBufferSize = lshBufferSize
    ActorBasedPartitionedHTreeMap.totalFeedingThreads = threadNumber
    htreeDebug = conf.getBoolean("cpslab.lsh.htree.debug")

    val readThreadNum = conf.getInt("cpslab.lsh.benchmark.readingThreadNum")

    initializeActorBasedHashTree(conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.dispatcher

    val cap = conf.getInt("cpslab.lsh.benchmark.asyncCap")
    val readCap = conf.getInt("cpslab.lsh.benchmark.asyncReadCap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val replica = conf.getInt("cpslab.lsh.benchmark.replica")
    val ifRunRead = conf.getBoolean("cpslab.lsh.benchmark.ifRunReadTest")
    ActorBasedPartitionedHTreeMap.tableNum = tableNum
    /*
    def traverseAllFiles(): Unit = {
      for (i <- 0 until threadNumber) {
        new Thread(new Runnable {
          val base = i
          override def run(): Unit = {
            var cnt = 0
            val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))
            for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
              if (cnt >= cap) {
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
        readCap * readThreadNum, ifRunRead)),
      name = "monitor")
    traverseAllFiles()
    */
    val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))

    var cnt = 0

    // val taskQueue = fillTaskQueue(allFiles, cap * threadNumber)
    // println(s"finished loading ${taskQueue.length} vectors")
    val listBuffer = new ListBuffer[ValueAndHash]
    val startTime = System.nanoTime()
    for (i <- 0 until 500000) {
      val vector = i
      // println(vector.vectorId)
      val h = vectorIdToVector.hash(i)
      vectorIdToVector.partitioner.getPartition(i)
      // listBuffer += ValueAndHash(vector, h)
    }
    val endTime = System.nanoTime()
    println(endTime - startTime)
    ActorBasedPartitionedHTreeMap.actorSystem.awaitTermination()
  }

  val finishedWriteThreadCount = new AtomicInteger(0)
  val trainingIDs = new ListBuffer[Int]
  val testIDs = new ListBuffer[Int]

  def fillTaskQueue(allFiles: Seq[String], totalAmount: Int): List[SparseVector] = {
    var cnt = 0
    val taskQueue = new ListBuffer[SparseVector]
    for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
      val (_, size, indices, values) = Vectors.fromString1(line)
      val squareSum = math.sqrt(values.foldLeft(0.0) {
        case (sum, weight) => sum + weight * weight
      })
      val vector = new SparseVector(cnt, size, indices,
        values.map(_ / squareSum))
      taskQueue += vector
      cnt += 1
      if (cnt >= totalAmount) {
        return taskQueue.toList
      }
    }
    List[SparseVector]()
  }

  def testWriteThreadScalabilityWithMapDB(conf: Config,
                                          threadNumber: Int): Unit = {
    ShardDatabase.initializeMapDBHashMap(conf)
    startWriteWorkload(conf, threadNumber)
  }

  private def calculateFirstLevelHashForBTree(completeHashKey: Long): Long = {
    val key = completeHashKey >>> (BTreeDatabase.btreeCompareGroupNum - 1) *
      BTreeDatabase.btreeCompareGroupLength
    // compose level bits in the first level by moving the 1L to 9th position and
    val level = 1L << BTreeDatabase.btreeCompareGroupLength
    level | key
  }

  private def startWriteWorkloadToBTree(conf: Config, threadNumber: Int): Unit = {
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")

    val compareGroupLength = conf.getInt("cpslab.lsh.btree.compareGroupLength")
    val compareGroupNum = conf.getInt("cpslab.lsh.btree.compareGroupNum")
    val maxNodeNum = conf.getInt("cpslab.lsh.btree.maximumNodeNum")
    val debug = conf.getBoolean("cpslab.lsh.btree.debug")
    val appendDebug = conf.getBoolean("cpslab.lsh.btree.appendDebug")
    val debugVectorMax = conf.getInt("cpslab.lsh.btree.debugVectorMax")
    val instrumentError = conf.getBoolean("cpslab.lsh.btree.instrumentError")
    BTreeDatabase.debug = debug
    BTreeDatabase.btreeCompareGroupLength = compareGroupLength
    BTreeDatabase.btreeCompareGroupNum = compareGroupNum
    BTreeDatabase.btreeMaximumNode = maxNodeNum
    BTreeDatabase.instrumentError = instrumentError

    val random = new Random(System.currentTimeMillis())
    val allFiles = random.shuffle(Utils.buildFileListUnderDirectory(filePath))

    val taskQueue = fillTaskQueue(allFiles, cap * threadNumber)
    ActorBasedPartitionedHTreeMap.actorSystem = ActorSystem("AK", conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.dispatchers.lookup(
      "akka.actor.writer-dispatcher")
    val st = System.nanoTime()
    val mainFs = taskQueue.map {
      vector =>
        val f1 = Future {
          val vId = {
            if (debug) {
              Random.nextInt(debugVectorMax)
            } else {
              vector.vectorId
            }
          }
          vectorIdToVectorBTree.putWithDebugging(vId, vector, appendDebug)
          vector
        }
        if (debug) {
          f1
        } else {
          f1.flatMap {
            returnedVector =>
              val fs = (0 until tableNum).map(tableId => {
                Future {
                  val lshCalculator = HashTreeTest.lshEngines(tableId)
                  if (lshCalculator == null) {
                    println(s"FAULT: lshcalculator for table $tableId is null")
                  }
                  // to be equivalent to the MapDB.hash()
                  val v = vectorIdToVectorBTree.get(returnedVector.vectorId)
                  if (v == null) {
                    println(s"found ${returnedVector.vectorId} as null")
                  }
                  val h = lshCalculator.hash(returnedVector, Serializers.VectorSerializer)
                  val h1 = lshPartitioners(tableId).getPartition(h)
                  val lh = h & 0xffffffffL
                  //get the first group
                  val key = calculateFirstLevelHashForBTree(lh)
                  vectorDatabaseBTree(tableId).append(key,
                    new LSHBTreeVal(returnedVector.vectorId, lh), 0)
                }
              })
              Future.sequence(fs)
          }
        }
    }
    Future.sequence(mainFs).onComplete {
      case Success(result)  =>
        // do nothing
        val duration = System.nanoTime() - st
        println("total write throughput: " +
          cap * threadNumber / (duration.toDouble / 1000000000))
        finishedWriteThreadCount.set(threadNumber)
      case Failure(failure) =>
        failure.printStackTrace()
        throw failure
    }
  }

  private def startWriteWorkload(conf: Config, threadNumber: Int): Unit = {
    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val random = new Random(System.currentTimeMillis())
    val allFiles = random.shuffle(Utils.buildFileListUnderDirectory(filePath))

    var cnt = 0

    var taskQueue = fillTaskQueue(allFiles, cap * threadNumber)
    println(s"writing ${taskQueue.size} vectors")
    ActorBasedPartitionedHTreeMap.actorSystem = ActorSystem("AK", conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.dispatchers.lookup(
      "akka.actor.writer-dispatcher")
    val st = System.nanoTime()
    val mainFs = taskQueue.map {
      vector =>
        Future {
          vectorIdToVector.put(vector.vectorId, vector)
        }.flatMap {
          returnedVector =>
            val fs = (0 until tableNum).map(tableId => {
              Future {
                vectorDatabase(tableId).put(returnedVector.vectorId, true)
              }
            })
            Future.sequence(fs)
        }
    }
    Future.sequence(mainFs).onComplete {
      case Success(result)  =>
        // do nothing
        val duration = System.nanoTime() - st
        println("total write throughput: " +
          cap * threadNumber / (duration.toDouble / 1000000000))
        taskQueue = List[SparseVector]()
        finishedWriteThreadCount.set(threadNumber)
        println(s"conflict count:" +
          s" ${vectorDatabase(0).asInstanceOf[ActorPartitionedHTreeBasic[Int, Boolean]].
            redistributionCount}" + " " +
          s"${vectorIdToVector.asInstanceOf[ActorPartitionedHTreeBasic[Int, SparseVector]].
            redistributionCount}")
      case Failure(failure) =>
        throw failure
    }
  }

  def testWriteThreadScalability(
    conf: Config,
    threadNumber: Int): Unit = {
    val dbType = conf.getString("cpslab.lsh.benchmark.dbtype")

    if (dbType == "partitionedHashMap") {
      println("partitionedHashMap initialized")
      ShardDatabase.initializePartitionedHashMap(conf)
    } else if (dbType == "mapdbHashMap") {
      println("mapdbHashMap initialized")
      ShardDatabase.initializeMapDBHashMap(conf)
    } else if (dbType == "btree") {
      initBTreeMap(conf, threadNumber)
    }

    if (dbType != "btree") {
      startWriteWorkload(conf, threadNumber)
    } else {
      startWriteWorkloadToBTree(conf, threadNumber)
    }
  }

  def asyncTestReadThreadScalability(conf: Config,
                                     requestNumberPerThread: Int): Unit = {
    val actorNumPerPartition = conf.getInt("cpslab.lsh.benchmark.readerActorNum")
    ActorBasedPartitionedHTreeMap.readerActorsNumPerPartition = actorNumPerPartition
    val bufferSize = conf.getInt("cpslab.lsh.benchmark.readBufferSize")
    ActorBasedPartitionedHTreeMap.readBufferSize = bufferSize
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
              val partitionId = table.getPartition(hash)
              val segId = hash >>> table.BUCKET_LENGTH
              val actorId = math.abs(s"$tableId-$segId".hashCode) %
                ActorBasedPartitionedHTreeMap.readerActorsNumPerPartition
              if (bufferSize > 0) {
                val actorIndex = s"$partitionId-$actorId"
                val bufferLock = bufferLocks(actorIndex).writeLock()
                try {
                  bufferLock.lock()
                  buffer(actorIndex) += Tuple2(tableId, interestVectorId)
                  if (buffer(actorIndex).length >= ActorBasedPartitionedHTreeMap.readBufferSize) {
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
          println(s"thread $t finished sending read requests")
          ActorBasedPartitionedHTreeMap.stoppedReadingThreads.getAndIncrement()
          if (ActorBasedPartitionedHTreeMap.stoppedReadingThreads.get() ==
            ActorBasedPartitionedHTreeMap.totalReadingThreads) {
            dumpBuffer()
          }
        }
      })
    }
  }

  def testReadThreadScalability(
      conf: Config,
      requestNumberPerThread: Int,
      threadNumber: Int): Unit = {

    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val dbType = conf.getString("cpslab.lsh.benchmark.dbtype")
    // pure thread/future is too clean as a baseline
    // against the complicate akka (bring too much overhead)

    System.gc()
    //ActorBasedPartitionedHTreeMap.actorSystem = ActorSystem("AK", conf)
    implicit val executionContext = ActorBasedPartitionedHTreeMap.actorSystem.
      dispatchers.lookup("akka.actor.default-dispatcher")

    val interestVectorIds = {
      val l = new ListBuffer[(SparseVector, Int)]
      for (i <- 0 until requestNumberPerThread * threadNumber) {
        val vectorId = Random.nextInt(cap * threadNumber)
        val vector = {
          if (dbType != "btree") {
            ShardDatabase.vectorIdToVector.get(vectorId)
          } else {
            ShardDatabase.vectorIdToVectorBTree.get(vectorId)
          }
        }
        for(tableId <- 0 until tableNum) {
          l += Tuple2(vector, tableId)
        }
      }
      l.toList
    }

    val fs = interestVectorIds.map {case (vector, tableId) =>
        Future {
          if (dbType != "btree") {
            ShardDatabase.vectorDatabase(tableId).getSimilar(vector.vectorId)
          } else {
            // b-tree
            //1. fetch the corresponding vector
            //2. calculate the lsh value
            if (vector == null) {
              println(s"get ${vector.vectorId} as null")
            } else {
              //3. get from vectorDatabase
              val v = {
                //if (tableId == 0) {
                  ShardDatabase.vectorIdToVectorBTree.get(vector.vectorId)
                //} else {
                  //vector
                //}
              }
              val h = lshEngines(tableId).hash(vector, Serializers.VectorSerializer).toLong
              for (i <- 0 until BTreeDatabase.btreeCompareGroupNum) {
                ShardDatabase.vectorDatabaseBTree(tableId).getAll(h >>>
                  (i * BTreeDatabase.btreeCompareGroupLength))
              }
            }
          }
        }
    }
    val st = System.nanoTime()
    Future.sequence(fs).onComplete {
      case Success(result)  =>
      // do nothing
        val duration = System.nanoTime() - st
        println("total read throughput: " +
          requestNumberPerThread * threadNumber / (duration.toDouble / 1000000000))
      case Failure(failure) =>
        throw failure
    }
    ActorBasedPartitionedHTreeMap.actorSystem.awaitTermination()
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

  def initBTreeMap(conf: Config, threadNumber: Int): Unit = {
    ShardDatabase.initializeBTree(conf)
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    //initialize lsh engine
    HashTreeTest.lshEngines = (for (i <- 0 until tableNum)
      yield new LocalitySensitiveHasher(LSHServer.getLSHEngine, i)).toArray
    HashTreeTest.lshPartitioners = (for (i <- 0 until tableNum)
      yield new LocalitySensitivePartitioner[Int](confForPartitioner, i, partitionBits)).toArray
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
        0
      }
    }
    (efficiency, kNN, ifOverHit)
  }

  private def percentileDist(unSortedList: ListBuffer[Double]):
      (Double, Double, Double, Double, Double) = {
    val list = unSortedList.sortWith((a, b) => a < b)
    val a = list((list.length * 0.05).toInt)
    val b = list((list.length * 0.25).toInt)
    val c = list((list.length * 0.5).toInt)
    val d = list((list.length * 0.75).toInt)
    val e = list((list.length * 0.95).toInt)
    (a, b, c, d, e)
  }

  def testAccuracy(conf: Config): Unit = {
    import scala.collection.JavaConversions._
    val ratiosInstances = new ListBuffer[Double]
    val effSumInstances = new ListBuffer[Double]
    val experimentalInstances = conf.getInt("cpslab.expInstance")
    val overHitInstances = new Array[Int](experimentalInstances)
    val efficiencyDist = new ListBuffer[(Double, Double, Double, Double, Double)]
    for (exp <- 0 until experimentalInstances) {
      var ratio = 0.0
      var efficiencySum = new ListBuffer[Double]
      val tableNum = conf.getInt("cpslab.lsh.tableNum")
      val ifFixRequests = conf.getBoolean("cpslab.lsh.benchmark.accuracy.fixRequests")
      val totalCnt = conf.getInt("cpslab.lsh.benchmark.accuracy.totalCnt")
      val readFromTrainingSet = conf.getBoolean("cpslab.lsh.benchmark.accuracy.readFromTrainingSet")
      for (testCnt <- 0 until totalCnt) {
        val order = {
          if (ifFixRequests) {
            testCnt
          } else {
            if (!readFromTrainingSet) {
              Random.nextInt(testIDs.size)
            } else {
              Random.nextInt(testIDs.size + trainingIDs.size)
            }
          }
        }
        val queryVector = {
          if (!readFromTrainingSet) {
            vectorIdToVector.get(testIDs(order))
          } else {
            if (order > trainingIDs.size - 1) {
              vectorIdToVector.get(testIDs(order - trainingIDs.size))
            } else {
              vectorIdToVector.get(trainingIDs(order))
            }
          }
        }
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
        val itr = (trainingIDs ++ testIDs).iterator
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
          val r = sum / mostK
          if (r.isNaN) {
            println(s"FAULT: ratio $r ${queryVector.vectorId}")
            System.exit(1)
          }
          r
        }
        if (ratio.isNaN) {
          println(s"FAULT: ratio $ratio ${queryVector.vectorId}")
          System.exit(1)
        }
      }
      //println(ratio / totalCnt)
      //println("efficiency:" + efficiencySum.sum)
      ratiosInstances += ratio / totalCnt
      if (ratiosInstances(exp).isNaN) {
        println(s"FAULT: ratio $ratiosInstances, totalCnt $totalCnt, Ratio $ratio")
        System.exit(1)
      }
      effSumInstances += efficiencySum.sum
      //analyze efficiency distribution
      efficiencyDist += percentileDist(efficiencySum)
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
    println("efficiencyDist:" + efficiencyDist.toList)
  }

  def loadFiles(files: Seq[String], updateExistingID: ListBuffer[Int], tableNum: Int): Unit = {
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
    for (i <- 0 until updateExistingID.length) {
      assert(vectorIdToVector.get(updateExistingID(i)) != null)
    }
  }

  def loadAccuracyTestFiles(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")

    ShardDatabase.initializePartitionedHashMap(conf)

    val trainingPath = conf.getString("cpslab.lsh.trainingPath")
    val testPath = conf.getString("cpslab.lsh.testPath")
    val allTrainingFiles = Utils.buildFileListUnderDirectory(trainingPath)
    val allTestFiles = Utils.buildFileListUnderDirectory(testPath)
    loadFiles(allTrainingFiles, trainingIDs, tableNum)
    println(s"loaded training files + ${trainingIDs.length}")
    loadFiles(allTestFiles, testIDs, tableNum)
    println(s"loaded test files + ${testIDs.length}")
  }

  private def startTestAccuracy(conf: Config): Unit = {
    loadAccuracyTestFiles(conf)
    println("redistribution count:" +
      vectorDatabase(0).asInstanceOf[ActorPartitionedHTreeBasic[Int, Boolean]].redistributionCount)
    testAccuracy(conf)
  }

  private def startTestParallel(ifAsync: Boolean, conf: Config): Unit = {
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")
    if (ifAsync) {
      asyncTestWriteThreadScalability(conf, threadNumber)
    } else {
      val requestPerThread = conf.getInt("cpslab.lsh.benchmark.syncReadCap")
      val readThreadNum = conf.getInt("cpslab.lsh.benchmark.readingThreadNum")
      testWriteThreadScalability(conf, threadNumber)
      val ifRunRead = conf.getBoolean("cpslab.lsh.benchmark.ifRunReadTest")
      while (finishedWriteThreadCount.get() < threadNumber) {
        Thread.sleep(10000)
      }
      if (ifRunRead) {
        println("======read performance======")
        testReadThreadScalability(conf, requestPerThread, readThreadNum)
      }
    }
  }


  private def startTestStorage(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    persistWorkingDir = System.currentTimeMillis() + "/"
    new File(persistWorkingDir).mkdir()
    usePersistSegment = conf.getBoolean("cpslab.lsh.bechmark.storage.usePersistSegment")
    // preload vector
    startTestParallel(ifAsync = false, conf: Config)
    // write test
    val requestNum = conf.getInt("cpslab.lsh.benchmark.storage.requestNum")
    val vectorCnt = conf.getInt("cpslab.lsh.benchmark.threadNumber") *
      conf.getInt("cpslab.lsh.benchmark.cap")
    var sum = 0.0
    for (i <- 0 until requestNum) {
      val vId = Random.nextInt(vectorCnt)
      val v = vectorIdToVector.get(vId)
      val startTime = System.nanoTime()
      vectorIdToVector.put(v.vectorId, v)
      for (i <- 0 until tableNum) {
        vectorDatabase(i).put(v.vectorId, true)
      }
      val duration = (System.nanoTime() - startTime).toDouble / 1000000000
      sum += duration
    }
    println("average latency: " + sum)
  }

  private def startReadTest(conf: Config): Unit = {
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")
    val reqCnt = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")

    ShardDatabase.initializePartitionedHashMap(conf)

    startWriteWorkload(conf, threadNumber)

    while (finishedWriteThreadCount.get() < threadNumber) {
      Thread.sleep(10000)
    }

    for (i <- 0 until threadNumber) {
      new Thread(new Runnable {
        val latencyRecords = new ListBuffer[Long]
        override def run(): Unit = {
          val overAllStartTime = System.nanoTime()
          for (reqId <- 0 until reqCnt) {
            val startTime = System.nanoTime()
            for (tableId <- 0 until tableNum) {
              vectorDatabase(tableId).getSimilar(Random.nextInt(threadNumber * reqCnt))
            }
            latencyRecords += System.nanoTime() - startTime
          }
          val overAllEndTime = System.nanoTime()
          println(s"Thread: ${Thread.currentThread().getName} " +
            s"averageTime: ${(overAllEndTime - overAllStartTime)/1000000000} seconds, " +
            s"max: ${latencyRecords.max / 1000000000} seconds," +
            s" min: ${latencyRecords.min / 1000000000} seconds")
      }}).start()
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    LSHServer.lshEngine = new LSH(conf)
    println("=====initialized LSHEngine=====")
    // val allTestFiles = Utils.buildFileListUnderDirectory(testPath)
    //initializeActorBasedHashTree(conf)
    /*ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.cap"),
      conf.getInt("cpslab.lsh.tableNum"))*/

    
    ActorBasedPartitionedHTreeMap.shareActor = args(2).toBoolean

    val testMode = args(3)

    testMode match {
      case "accuracy" =>
        startTestAccuracy(conf)
      case "parallel" =>
        startTestParallel(args(1) == "async", conf)
      case "storage" =>
        startTestStorage(conf)
      case "read" =>
        startReadTest(conf)
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
