package cpslab.db

import java.util.concurrent.ExecutorService
import java.util.concurrent.atomic.AtomicInteger

import cpslab.deploy.ShardDatabase

import scala.collection.mutable
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

  private class WriterActor(partitionId: Int) extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    var earliestStartTime = Long.MaxValue
    var latestEndTime = Long.MinValue

    override def preStart(): Unit = {
      while (ActorBasedPartitionedHTreeMap.stoppedFeedingThreads.get() < 10) {
        Thread.sleep(1000)
      }
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
        for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
          ActorBasedPartitionedHTreeMap.chooseRandomActor(partitioner.numPartitions) !
            Dispatch(tableId, vectorId)
        }
      } else {
        for (tableId <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
          chooseRandomActor ! Dispatch(tableId, vectorId)
        }
      }
    }

    override def receive: Receive = {
      case Dispatch(tableId: Int, vectorId: Int) =>
        earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        vectorDatabase(tableId).put(vectorId, true)
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
            Tuple2(earliestStartTime, latestEndTime)
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
        writerActors(partition)(
          math.abs(s"$tableId-$segmentId".hashCode) % writerActorsNumPerPartition) !
          ValueAndHash(value.asInstanceOf[SparseVector], h)
      } else {
        actors(partition)(segmentId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
      }
    } else {
      if (shareActor) {
        writerActors(partition)(
          math.abs(s"$tableId-$segmentId".hashCode) % writerActorsNumPerPartition) !
          KeyAndHash(tableId, key.asInstanceOf[Int], h)
      } else {
        actors(partition)(segmentId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
      }
    }
    value
  }
}

final case class PerformanceReport(throughput: Double)
final case class ValueAndHash(vector: SparseVector, hash: Int)
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

  def chooseRandomActor(partitionNum: Int): ActorRef = {
    val partitionId = Random.nextInt(partitionNum)
    val actors = writerActors(partitionId)
    actors(Random.nextInt(actors.length))
  }

  // just for testing
  var stoppedFeedingThreads = new AtomicInteger(0)
}
