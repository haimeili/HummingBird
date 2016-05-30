package cpslab.db

import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.{ConcurrentHashMap, ExecutorService}
import java.util.concurrent.atomic.AtomicInteger

import cpslab.deploy.ShardDatabase

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
  // partitionId-actorIndex -> (vectorId, hash)
  private val bufferOfLSHTable = new mutable.HashMap[String, ListBuffer[(Int, Int)]]
  private val bufferOfLSHTableLocks = new mutable.HashMap[String, ReentrantReadWriteLock]()

  private class WriterActor(partitionId: Int) extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    var earliestStartTime = Long.MaxValue
    var latestEndTime = Long.MinValue
    var mainTableMsgCnt = 0
    var lshTableMsgCnt = 0

    override def preStart(): Unit = {
      while (ActorBasedPartitionedHTreeMap.stoppedFeedingThreads.get() <
        ActorBasedPartitionedHTreeMap.totalFeedingThreads) {
        Thread.sleep(1000)
      }
      //for main table
      vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].dumpMainTableBuffer()
      /*
      for (i <- 0 until vectorDatabase.length) {
        vectorDatabase(i).asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].dumpLSHTableBuffer()
      }*/
    }

    import ActorBasedPartitionedHTreeMap._

    private def chooseRandomActor: ActorRef = {
      val tableId = Random.nextInt(vectorDatabase.length)
      val partitionId = Random.nextInt(partitioner.numPartitions)
      val actors = vectorDatabase(tableId).asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].
        actors(partitionId)
      actors(Random.nextInt(actors.length))
    }

    private def dispatchLSHCalculation(vectorId: Int): Unit = {
      if (shareActor) {
        if (parallelLSHComputation) {
          for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
            ActorBasedPartitionedHTreeMap.chooseRandomActor(partitioner.numPartitions) !
              Dispatch(tableId, vectorId)
          }
        } else {
          for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
            vectorDatabase(tableId).put(vectorId, true)
          }
        }
      } else {
        if (parallelLSHComputation) {
          for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
            chooseRandomActor ! Dispatch(tableId, vectorId)
          }
        } else {
          for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
            vectorDatabase(tableId).put(vectorId, true)
          }
        }
      }
    }

    private def processingBatchKeyAndHash(batch: BatchKeyAndHash): Unit = {
      for ((vectorId, h) <- batch.batch) {
        if (shareActor) {
          vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].putExecuteByActor(
            partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        } else {
          putExecuteByActor(
            partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        }
      }
    }

    private def processingBatchValueAndHash(batch: BatchValueAndHash): Unit = {
      for ((vector, h) <- batch.batch) {
        mainTableMsgCnt += 1
        if (shareActor) {
          vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].putExecuteByActor(
            partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        } else {
          putExecuteByActor(
            partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        }
        dispatchLSHCalculation(vector.vectorId)
      }
    }

    override def receive: Receive = {
      case Dispatch(tableId: Int, vectorId: Int) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        vectorDatabase(tableId).put(vectorId, true)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case b @ BatchValueAndHash(batch: List[(SparseVector, Int)]) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        processingBatchValueAndHash(b)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case b @ BatchKeyAndHash(batch: List[(Int, Int)]) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        processingBatchKeyAndHash(b)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case ValueAndHash(vector: SparseVector, h: Int) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        if (shareActor) {
          vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].putExecuteByActor(
            partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        } else {
          putExecuteByActor(
            partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        }
        dispatchLSHCalculation(vector.vectorId)
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case KeyAndHash(tableId: Int, vectorId: Int, h: Int) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        if (shareActor) {
          lshTableMsgCnt += 1
          vectorDatabase(tableId).asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].
            putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        } else {
          putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        }
        latestEndTime = math.max(latestEndTime, System.nanoTime())
      case ReceiveTimeout =>
        if (earliestStartTime != Long.MaxValue || latestEndTime != Long.MinValue) {
          // context.actorSelection("akka://AK/user/monitor") ! PerformanceReport(totalMsgs * 1.0 /
            // ((latestEndTime - earliestStartTime) * 1.0 / 1000000000))
          context.actorSelection("akka://AK/user/monitor") !
            Tuple4(earliestStartTime, latestEndTime, mainTableMsgCnt, lshTableMsgCnt)
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
            Props(new WriterActor(partitionId)))
        }
      }
    }
  } else {
    for (partitionId <- 0 until partitioner.numPartitions) {
      val actorNum = math.pow(2, 32 - PartitionedHTreeMap.BUCKET_LENGTH).toInt
      actors.put(partitionId, new Array[ActorRef](actorNum))
      for (segmentId <- 0 until actorNum) {
        actors(partitionId)(segmentId) = ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
          Props(new WriterActor(partitionId)))
      }
    }
  }

  if (shareActor) {
    if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
      // main table
      for (partitionId <- 0 until partitioner.numPartitions) {
        for (i <- 0 until writerActorsNumPerPartition) {
          val id = s"$partitionId-$i"
          bufferOfMainTable.put(id, new ListBuffer[(SparseVector, Int)])
          bufferOfMainTableLocks.put(id, new ReentrantReadWriteLock())
        }
      }
    } else {
      // lsh table
      for (partitionId <- 0 until partitioner.numPartitions) {
        for (i <- 0 until writerActorsNumPerPartition) {
          val id = s"$partitionId-$i"
          bufferOfLSHTable.put(id, new ListBuffer[(Int, Int)])
          bufferOfLSHTableLocks.put(id, new ReentrantReadWriteLock())
        }
      }
    }
  }

  def dumpLSHTableBuffer(): Unit = synchronized {
    for ((id, buffer) <- bufferOfLSHTable) {
      val lock = bufferOfLSHTableLocks(id).writeLock()
      try {
        lock.lock()
        val Array(partitionId, actorId) = id.split("-")
        writerActors(partitionId.toInt)(actorId.toInt) ! BatchKeyAndHash(buffer.toList)
        bufferOfLSHTable(id) = new ListBuffer[(Int, Int)]
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        lock.unlock()
      }
    }
  }

  def dumpMainTableBuffer(): Unit = synchronized {
    for ((id, buffer) <- bufferOfMainTable) {
      val lock = bufferOfMainTableLocks(id).writeLock()
      try {
        lock.lock()
        val Array(partitionId, actorId) = id.split("-")
        writerActors(partitionId.toInt)(actorId.toInt) ! BatchValueAndHash(buffer.toList)
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
    val seg: Int = h >>> PartitionedHTreeMap.BUCKET_LENGTH
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

  private def bufferingPutForLSHTable(partitionId: Int, actorId: Int, vectorId: Int,
                                      h: Int): Unit = {
    val bufferLock = bufferOfLSHTableLocks(s"$partitionId-$actorId").writeLock()
    try {
      bufferLock.lock()
      val buffer = bufferOfLSHTable(s"$partitionId-$actorId")
      buffer += (vectorId -> h)
      if (buffer.size >= bufferSize) {
        writerActors(partitionId)(actorId) ! BatchKeyAndHash(buffer.toList)
        bufferOfLSHTable(s"$partitionId-$actorId") = new ListBuffer[(Int, Int)]
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      bufferLock.unlock()
    }
  }

  private def bufferingPutForMainTable(partitionId: Int, actorId: Int, vector: SparseVector,
                                       h: Int): Unit = {
    val bufferLock = bufferOfMainTableLocks(s"$partitionId-$actorId").writeLock()
    try {
      bufferLock.lock()
      val buffer = bufferOfMainTable(s"$partitionId-$actorId")
      buffer += (vector.asInstanceOf[SparseVector] -> h)
      if (buffer.size >= bufferSize) {
        writerActors(partitionId)(actorId) ! BatchValueAndHash(buffer.toList)
        bufferOfMainTable(s"$partitionId-$actorId") = new ListBuffer[(SparseVector, Int)]
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    } finally {
      bufferLock.unlock()
    }
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
    val segmentId = h >>> PartitionedHTreeMap.BUCKET_LENGTH
    if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
      if (shareActor) {
        val actorId = math.abs(s"$tableId-$segmentId".hashCode) % writerActorsNumPerPartition
        if (bufferSize <= 0) {
          writerActors(partition)(actorId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
        } else {
          bufferingPutForMainTable(partition, actorId, value.asInstanceOf[SparseVector], h)
        }
      } else {
        actors(partition)(segmentId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
      }
    } else {
      if (shareActor) {
        val actorId = math.abs(s"$tableId-$segmentId".hashCode) % writerActorsNumPerPartition
        writerActors(partition)(actorId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
        /*
        if (bufferSize <= 0) {
          writerActors(partition)(actorId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
        } else {
          bufferingPutForLSHTable(partition, actorId, key.asInstanceOf[Int], h)
        }*/
      } else {
        actors(partition)(segmentId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
      }
    }
    value
  }
}

final case class PerformanceReport(throughput: Double)
final case class ValueAndHash(vector: SparseVector, hash: Int)
final case class BatchValueAndHash(batch: List[(SparseVector, Int)])
final case class BatchKeyAndHash(batch: List[(Int, Int)])
final case class Dispatch(tableId: Int, vectorId: Int)
final case class KeyAndHash(tableId: Int, vectorId: Int, hash: Int)

object ActorBasedPartitionedHTreeMap {
  var actorSystem: ActorSystem = null
  var tableNum: Int = 0

  var histogramOfSegments: Array[Array[Array[Int]]] = null
  var histogramOfPartitions: Array[Array[Int]] = null

  //new mutable.HashMap[Int, Array[ActorRef]]
  var writerActors: mutable.HashMap[Int, Array[ActorRef]] = null
  var writerActorsNumPerPartition: Int = 0

  var shareActor = true
  var parallelLSHComputation = false
  var bufferSize = 0

  def chooseRandomActor(partitionNum: Int): ActorRef = {
    val partitionId = Random.nextInt(partitionNum)
    val actors = writerActors(partitionId)
    actors(Random.nextInt(actors.length))
  }

  // just for testing
  var stoppedFeedingThreads = new AtomicInteger(0)
  var totalFeedingThreads = 0
}
