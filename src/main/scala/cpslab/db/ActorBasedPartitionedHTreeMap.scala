package cpslab.db

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentHashMap, ExecutorService}
import java.util.concurrent.atomic.AtomicInteger

import cpslab.deploy.ShardDatabase
import cpslab.deploy.benchmark.HashTreeTest
import cpslab.utils.LocalitySensitivePartitioner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.ShardDatabase._
import cpslab.lsh.LocalitySensitiveHasher
import cpslab.lsh.vector.SparseVector

import scala.util.Random

class ActorBasedPartitionedHTreeMap[K, V](
    conf: Config,
    tableId: Int,
    hasherName: String,
    workingDirectory: String,
    name: String,
    partitioner: Partitioner[K],
    closeEngine: Boolean,
    hashSalt: Int,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    valueCreator: Fun.Function1[V, K],
    executor: ExecutorService,
    closeExecutor: Boolean,
    ramThreshold: Long)
  extends ActorPartitionedHTreeBasic[K, V](
    tableId,
    hasherName,
    workingDirectory,
    name,
    partitioner,
    closeEngine,
    hashSalt,
    keySerializer,
    valueSerializer,
    valueCreator,
    executor,
    closeExecutor,
    ramThreshold) {

  // partitionId-actorIndex -> (SparseVector, hash)
  private val bufferOfMainTable = new mutable.HashMap[String, ListBuffer[(SparseVector, Int)]]
  private val bufferOfMainTableLocks = new mutable.HashMap[String, ReentrantReadWriteLock]()

  private class ReaderActor(partitionId: Int) extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    var earliestStartTime = Long.MaxValue
    var latestEndTime = Long.MinValue

    override def preStart(): Unit = {
      while (ActorBasedPartitionedHTreeMap.stoppedReadingThreads.get() <
        ActorBasedPartitionedHTreeMap.totalReadingThreads) {
        Thread.sleep(100)
      }
    }

    private def processingQueryRequest(tableId: Int, vectorKey: Int): Unit = {
      msgCnt += 1
      vectorDatabase(tableId).getSimilar(vectorKey)
    }

    private var msgCnt = 0
    private var batchMsgCnt = 0

    private def processingBatchQueryRequest(batch: List[(Int, Int)]): Unit = {
      batchMsgCnt += 1
      for ((tableId, vectorId) <- batch) {
        msgCnt += 1
        vectorDatabase(tableId).getSimilar(vectorId)
      }
    }

    override def receive: Receive = {
      case BatchQueryRequest(batch: List[(Int, Int)]) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        processingBatchQueryRequest(batch)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case QueryRequest(tableId: Int, vectorKey: Int) =>
        //NOTE: it is just for performance testing,
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        processingQueryRequest(tableId, vectorKey)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case ReceiveTimeout =>
        if (earliestStartTime != Long.MaxValue || latestEndTime != Long.MinValue) {
          context.actorSelection("akka://AK/user/monitor") !
            ReadPerformanceReport(earliestStartTime, latestEndTime, msgCnt, batchMsgCnt)
        }
    }
  }

  private class WriterActor(partitionId: Int) extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    var latestEndTime = Long.MinValue
    var mainTableMsgCnt = 0
    var lshTableMsgCnt = 0
    var batchMainTableMsgCnt = 0
    var batchLSHTableMsgCnt = 0

    //actorIndex (partitionId-actorId) -> (tableId, vectorId, storageName)
    private val lshTableMsgBuffer = new mutable.HashMap[String, ListBuffer[KeyAndHash]]

    override def preStart(): Unit = {
      while (ActorBasedPartitionedHTreeMap.stoppedFeedingThreads.get() <
        ActorBasedPartitionedHTreeMap.totalFeedingThreads) {
        Thread.sleep(100)
      }
      //for main table
      vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].dumpMainTableBuffer()
    }

    import ActorBasedPartitionedHTreeMap._

    private def innerDispatchLSHCalculationWithSharedActor(vectorId: Int): Unit = {
      if (!HashTreeTest.htreeDebug) {
        for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
          if (lshBufferSize > 0) {
            val table = vectorDatabase(tableId).
              asInstanceOf[ActorBasedPartitionedHTreeMap[Int, Boolean]]
            val h = table.hash(vectorId)
            val segId = h >>> BUCKET_LENGTH
            val partitionId = table.getPartition(h)
            val actorId = math.abs(s"$tableId-$segId".hashCode) %
              ActorBasedPartitionedHTreeMap.writerActorsNumPerPartition
            val actorIndex = s"$partitionId-$actorId"
            if (!lshTableMsgBuffer.contains(actorIndex)) {
              lshTableMsgBuffer += actorIndex -> new ListBuffer[KeyAndHash]
            }
            lshTableMsgBuffer(actorIndex) += KeyAndHash(tableId, vectorId, h)
            if (lshTableMsgBuffer(actorIndex).size >= lshBufferSize) {
              writerActors(partitionId)(actorId) !
                BatchKeyAndHash(lshTableMsgBuffer(actorIndex).toList)
              lshTableMsgBuffer(actorIndex) = new ListBuffer[KeyAndHash]
            }
          } else {
            vectorDatabase(tableId).put(vectorId, true)
          }
        }
      }
    }

    private def innerDispatchLSHCalculationWithNonSharedActor(vectorId: Int): Unit = {
      for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
        if (lshBufferSize > 0) {
          val table = vectorDatabase(tableId).
            asInstanceOf[ActorBasedPartitionedHTreeMap[Int, Boolean]]
          val h = table.hash(vectorId)
          val segId = h >>> BUCKET_LENGTH
          val partitionId = table.getPartition(h)
          val actor = actors(partitionId)(segId)
          val actorIndex = s"$partitionId-$segId"
          if (!lshTableMsgBuffer.contains(actorIndex)) {
            lshTableMsgBuffer += actorIndex -> new ListBuffer[KeyAndHash]
          }
          lshTableMsgBuffer(actorIndex) += KeyAndHash(tableId, vectorId, h)
          if (lshTableMsgBuffer(actorIndex).size >= lshBufferSize) {
            actor ! BatchKeyAndHash(lshTableMsgBuffer(actorIndex).toList)
            lshTableMsgBuffer(actorIndex) = new ListBuffer[KeyAndHash]
          }
        } else {
          vectorDatabase(tableId).put(vectorId, true)
        }
      }
    }

    private def dispatchLSHCalculation(vectorId: Int): Unit = {
      if (shareActor) {
        innerDispatchLSHCalculationWithSharedActor(vectorId)
      } else {
        innerDispatchLSHCalculationWithNonSharedActor(vectorId)
      }
    }

    private def innerProcessingBatchKeyWithSharedActor(tableId: Int, vectorId: Int, hash: Int):
        Unit = {
      val table = vectorDatabase(tableId).asInstanceOf[ActorBasedPartitionedHTreeMap[Int,
        Boolean]]
      val segId = hash >>> BUCKET_LENGTH
      val storageName = table.initPartitionIfNecessary(partitionId, segId)
      val rootRecId = table.getRootRecId(partitionId, segId)
      val engine = table.storageSpaces.get(storageName)
      table.storeVector(vectorId, true, hash, rootRecId, partitionId, segId, engine)
    }

    private def innerProcessingBatchKeyWithNonSharedActor(tableId: Int, vectorId: Int, hash: Int):
        Unit = {
      innerProcessingBatchKeyWithSharedActor(tableId, vectorId, hash)
    }

    private def processingBatchKeyAndHash(batch: BatchKeyAndHash): Unit = {
      for (KeyAndHash(tableId, vectorId, hash) <- batch.batch) {
        lshTableMsgCnt += 1
        if (shareActor) {
          innerProcessingBatchKeyWithSharedActor(tableId, vectorId, hash)
        } else {
          innerProcessingBatchKeyWithNonSharedActor(tableId, vectorId, hash)
        }
      }
    }

    private def dumpLSHBuffer(): Unit = {
      for ((actorName, buffer) <- lshTableMsgBuffer) {
        val Array(partitionId, actorId) = actorName.split("-")
        if (buffer.nonEmpty) {
          writerActors(partitionId.toInt)(actorId.toInt) ! BatchKeyAndHash(buffer.toList)
          lshTableMsgBuffer(actorName) = new ListBuffer[KeyAndHash]
        }
      }
    }

    private def processingDumpedBatchValueAndHash(dumpBatch: DumpedBatchValueAndHash): Unit = {
      processingBatchValueAndHash(BatchValueAndHash(dumpBatch.batch))
      dumpLSHBuffer()
    }

    private def processingBatchValueAndHash(batch: BatchValueAndHash): Unit = {
      for ((vector, hashForMainTable) <- batch.batch) {
        mainTableMsgCnt += 1
        if (shareActor) {
          vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].putExecuteByActor(
            partitionId, hashForMainTable, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        } else {
          putExecuteByActor(
            partitionId, hashForMainTable, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        }
        dispatchLSHCalculation(vector.vectorId)
      }
    }

    private def processingValueAndHash(vector: SparseVector, h: Int): Unit = {
      if (shareActor) {
        vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].putExecuteByActor(
          partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
      } else {
        putExecuteByActor(
          partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
      }
      dispatchLSHCalculation(vector.vectorId)
      latestEndTime = math.max(latestEndTime, System.nanoTime())
    }

    private def processingKeyAndHash(tableId: Int, vectorId: Int, h: Int): Unit = {
      lshTableMsgCnt += 1
      if (shareActor) {
        vectorDatabase(tableId).asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].
          putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
      } else {
        putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
      }
      latestEndTime = math.max(latestEndTime, System.nanoTime())
    }

    override def receive: Receive = {
      case b @ BatchValueAndHash(batch: List[(SparseVector, Int)]) =>
        batchMainTableMsgCnt += 1
        processingBatchValueAndHash(b)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case b @ BatchKeyAndHash(batch: List[KeyAndHash]) =>
        batchLSHTableMsgCnt += 1
        processingBatchKeyAndHash(b)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case b @ DumpedBatchValueAndHash(batch: List[(SparseVector, Int)]) =>
        if (batch.nonEmpty) {
          batchMainTableMsgCnt += 1
        }
        processingDumpedBatchValueAndHash(b)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case ValueAndHash(vector: SparseVector, h: Int) =>
        mainTableMsgCnt += 1
        processingValueAndHash(vector, h)
      case KeyAndHash(tableId: Int, vectorId: Int, h: Int) =>
        processingKeyAndHash(tableId, vectorId, h)
      case ReceiveTimeout =>
        if (latestEndTime != Long.MinValue) {
          // context.actorSelection("akka://AK/user/monitor") ! PerformanceReport(totalMsgs * 1.0 /
            // ((latestEndTime - earliestStartTime) * 1.0 / 1000000000))
          context.actorSelection("akka://AK/user/monitor") !
            Tuple6(1, latestEndTime, mainTableMsgCnt, lshTableMsgCnt,
              batchMainTableMsgCnt, batchLSHTableMsgCnt)
          for ((actorName, lshBuffer) <- lshTableMsgBuffer) {
            if (lshBuffer.nonEmpty) {
              println(s"WARN::::lshBuffer is not empty at ${context.self.path.name}")
            }
          }
        }
    }
  }

  import ActorBasedPartitionedHTreeMap._

  val actors = new mutable.HashMap[Int, Array[ActorRef]]

  if (shareActor) {
    if (writerActors == null) {
      writerActors = new mutable.HashMap[Int, Array[ActorRef]]
      for (partitionId <- 0 until partitioner.numPartitions) {
        writerActors(partitionId) = new Array[ActorRef](writerActorsNumPerPartition)
        for (i <- 0 until writerActorsNumPerPartition) {
          writerActors(partitionId)(i) = ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
            Props(new WriterActor(partitionId)).withDispatcher("akka.actor.writer-dispatcher"),
            name = s"lshwriter-$partitionId-$i")
        }
      }
    }
  } else {
    for (partitionId <- 0 until partitioner.numPartitions) {
      val actorNum = math.pow(2, 32 - BUCKET_LENGTH).toInt
      actors.put(partitionId, new Array[ActorRef](actorNum))
      for (segmentId <- 0 until actorNum) {
        actors(partitionId)(segmentId) = ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
          Props(new WriterActor(partitionId)).withDispatcher("akka.actor.writer-dispatcher"),
          name = s"lshwriter-$tableId-$partitionId-$segmentId")
      }
    }
  }

  if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
    if (shareActor) {
      // main table
      for (partitionId <- 0 until partitioner.numPartitions) {
        for (i <- 0 until writerActorsNumPerPartition) {
          val id = s"$partitionId-$i"
          bufferOfMainTable.put(id, new ListBuffer[(SparseVector, Int)])
          bufferOfMainTableLocks.put(id, new ReentrantReadWriteLock())
        }
      }
    } else {
      for (partitionId <- 0 until partitioner.numPartitions) {
        for (i <- 0 until math.pow(2, 32 - BUCKET_LENGTH).toInt) {
          val id = s"$partitionId-$i"
          bufferOfMainTable.put(id, new ListBuffer[(SparseVector, Int)])
          bufferOfMainTableLocks.put(id, new ReentrantReadWriteLock())
        }
      }
    }
  }

  def dumpMainTableBuffer(): Unit = synchronized {
    for ((id, buffer) <- bufferOfMainTable if buffer.nonEmpty) {
      val lock = bufferOfMainTableLocks(id).writeLock()
      try {
        lock.lock()
        val Array(partitionId, actorId) = id.split("-")
        if (shareActor) {
          writerActors(partitionId.toInt)(actorId.toInt) ! DumpedBatchValueAndHash(buffer.toList)
        } else {
          actors(partitionId.toInt)(actorId.toInt) ! DumpedBatchValueAndHash(buffer.toList)
        }
        bufferOfMainTable(id) = new ListBuffer[(SparseVector, Int)]
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        lock.unlock()
      }
    }
  }

  //wrapper of putInner which is called inside the actor to update the state
  def putExecuteByActor(
      partition: Int,
      h: Int,
      key: K,
      value: V): Unit = {
    val seg: Int = {
      if (hasher.isInstanceOf[LocalitySensitiveHasher]) {
        h >>> BUCKET_LENGTH
      } else {
        h % SEG
      }
    }
    val storageName = buildStorageName(partition, seg)
    initPartitionIfNecessary(partition, seg)
    var ret: V = value
    try {
      partitionRamLock.get(storageName).writeLock.lock()
      ret = putInner(key, value, h, partition)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
    } finally {
      partitionRamLock.get(storageName).writeLock.unlock()
    }
  }

  // actorOrSegId - actorId, when sharing actor, segId otherwise
  private def bufferingPutForMainTable(partitionId: Int, actorOrSegId: Int, vector: SparseVector,
                                       h: Int): Unit = {
    val bufferLock = bufferOfMainTableLocks(s"$partitionId-$actorOrSegId").writeLock()
    try {
      bufferLock.lock()
      val buffer = bufferOfMainTable(s"$partitionId-$actorOrSegId")
      buffer += (vector.asInstanceOf[SparseVector] -> h)
      if (buffer.size >= bufferSize) {
        writerActors(partitionId)(actorOrSegId) ! BatchValueAndHash(buffer.toList)
        bufferOfMainTable(s"$partitionId-$actorOrSegId") = new ListBuffer[(SparseVector, Int)]
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      bufferLock.unlock()
    }
  }

  def getPartition(key: Int): Int = {
    partitioner.getPartition(key.asInstanceOf[K])
  }

  override def put(key: K, value: V): V = {
    if (key == null) {
      throw new IllegalArgumentException("null key")
    }

    if (value == null) {
      throw new IllegalArgumentException("null value")
    }

    val h: Int = hash(key)
    val partition: Int = partitioner.getPartition(
      (
        if (hasher.isInstanceOf[LocalitySensitiveHasher]) {
          h
        } else {
          key
        }
      ).asInstanceOf[K])
    if (partition < 0) {
      println(s"partition is less than 0 in table $tableId")
    }
    var segmentId = 0 // h >>> PartitionedHTreeMap.BUCKET_LENGTH
    if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
      segmentId = h % SEG
      if (shareActor) {
        val actorId = math.abs(s"$tableId-$segmentId".hashCode) % writerActorsNumPerPartition
        if (bufferSize <= 0) {
          writerActors(partition)(actorId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
        } else {
          bufferingPutForMainTable(partition, actorId, value.asInstanceOf[SparseVector], h)
        }
      } else {
        if (bufferSize <= 0) {
          actors(partition)(segmentId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
        } else {
          bufferingPutForMainTable(partition, segmentId, value.asInstanceOf[SparseVector], h)
        }
      }
    } else {
      segmentId = h >>> BUCKET_LENGTH
      if (shareActor) {
        val actorId = math.abs(s"$tableId-$segmentId".hashCode) %
          writerActorsNumPerPartition
        writerActors(partition)(actorId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
      } else {
        actors(partition)(segmentId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
      }
    }
    value
  }

  def getRootRecId(partitionId: Int, segId: Int): Long = {
    partitionRootRec.get(partitionId)(segId)
  }

  def initReaderActors(): Unit = {
    if (readerActors == null) {
      readerActors = new mutable.HashMap[Int, Array[ActorRef]]
      for (partitionId <- 0 until partitioner.numPartitions) {
        readerActors(partitionId) = new Array[ActorRef](readerActorsNumPerPartition)
        for (i <- 0 until readerActorsNumPerPartition) {
          readerActors(partitionId)(i) = ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
            Props(new ReaderActor(partitionId)), name = s"lshreader-$partitionId-$i")
        }
      }
    }
  }
}

final case class ReadPerformanceReport(startTime: Long, endTime: Long,
                                       msgCnt: Int, batchMsgCnt: Int)
final case class ValueAndHash(vector: SparseVector, hash: Int)
final case class BatchValueAndHash(batch: List[(SparseVector, Int)])
final case class DumpedBatchValueAndHash(batch: List[(SparseVector, Int)])
final case class BatchKeyAndHash(batch: List[KeyAndHash])
final case class Dispatch(tableId: Int, vectorId: Int)
final case class KeyAndHash(tableId: Int, vectorId: Int, hash: Int)
final case class QueryRequest(tableId: Int, key: Int)
final case class BatchQueryRequest(batch: List[(Int, Int)])

object ActorBasedPartitionedHTreeMap {
  var actorSystem: ActorSystem = null
  var tableNum: Int = 0

  var histogramOfSegments: Array[Array[Array[Int]]] = null
  var histogramOfPartitions: Array[Array[Int]] = null

  //new mutable.HashMap[Int, Array[ActorRef]]
  var writerActors: mutable.HashMap[Int, Array[ActorRef]] = null
  var writerActorsNumPerPartition: Int = 0
  var readerActors: mutable.HashMap[Int, Array[ActorRef]] = null
  var readerActorsNumPerPartition: Int = 0

  var shareActor = true
  var bufferSize = 0
  var lshBufferSize = 0
  var readBufferSize = 0

  def chooseRandomActor(partitionNum: Int): ActorRef = {
    val partitionId = Random.nextInt(partitionNum)
    val actors = writerActors(partitionId)
    actors(Random.nextInt(actors.length))
  }

  // just for testing
  var stoppedFeedingThreads = new AtomicInteger(0)
  var stoppedReadingThreads = new AtomicInteger(0)
  var totalFeedingThreads = 0
  var totalReadingThreads = 0
}
