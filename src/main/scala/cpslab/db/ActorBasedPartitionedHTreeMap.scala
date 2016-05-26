package cpslab.db

import java.util.concurrent.ExecutorService

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.ShardDatabase._
import cpslab.lsh.LocalitySensitiveHasher
import cpslab.lsh.vector.SparseVector

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

  private class WriterActor(partitionId: Int, segmentId: Int) extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    var totalTime = 0L
    var totalMsgs = 0L
    var sent = false
    override def receive: Receive = {
      case ValueAndHash(vector: SparseVector, h: Int) =>
        val s = System.nanoTime()
        vectorIdToVector.asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].
          putExecuteByActor(partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        for (i <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
          vectorDatabase(i).put(vector.vectorId, true)
        }
        val elapseTime = System.nanoTime() - s
        totalTime += elapseTime
        totalMsgs += 1
      case KeyAndHash(tableId: Int, vectorId: Int, h: Int) =>
        //earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        val s = System.nanoTime()
        vectorDatabase(tableId).asInstanceOf[ActorBasedPartitionedHTreeMap[K, V]].
          putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        val elapseTime = System.nanoTime() - s
        totalTime += elapseTime
        totalMsgs += 1
      case ReceiveTimeout =>
        if (!sent && totalMsgs != 0L) {
          context.actorSelection("akka://AK/user/monitor") ! PerformanceReport(totalMsgs * 1.0 /
            (totalTime * 1.0 / 1000000000))
          sent = true
        }
    }
  }

  import ActorBasedPartitionedHTreeMap._

  if (writerActors.size <= 0) {
    for (partitionId <- 0 until partitioner.numPartitions) {
      val actorNum = math.pow(2, 32 - PartitionedHTreeMap.BUCKET_LENGTH).toInt
      writerActors.put(partitionId, new Array[ActorRef](actorNum))
      for (segmentId <- 0 until actorNum) {
        writerActors(partitionId)(segmentId) = ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
          Props(new WriterActor(partitionId, segmentId)))
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
    var partition: Int = partitioner.getPartition(
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
      writerActors(partition)(segmentId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
    } else {
      writerActors(partition)(segmentId) ! KeyAndHash(tableId, key.asInstanceOf[Int], h)
    }
    value
  }
}

final case class PerformanceReport(throughput: Double)
final case class ValueAndHash(vector: SparseVector, hash: Int)
final case class KeyAndHash(tableId: Int, vectorId: Int, hash: Int)

object ActorBasedPartitionedHTreeMap {
  var actorSystem: ActorSystem = null
  var tableNum: Int = 0

  var histogramOfSegments: Array[Array[Array[Int]]] = null
  var histogramOfPartitions: Array[Array[Int]] = null

  val writerActors = new mutable.HashMap[Int, Array[ActorRef]]
}
