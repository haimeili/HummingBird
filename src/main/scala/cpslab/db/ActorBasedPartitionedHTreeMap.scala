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

    var earliestStartTime = Long.MaxValue
    var latestEndTime = Long.MinValue

    var sent = false
    override def receive: Receive = {
      case ValueAndHash(vector: SparseVector, h: Int) =>
        if (earliestStartTime == Long.MaxValue) {
          earliestStartTime = math.min(earliestStartTime, System.currentTimeMillis())
        }
        putExecuteByActor(partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        for (i <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
          vectorDatabase(i).put(vector.vectorId, true)
        }
      case KeyAndHash(vectorId: Int, h: Int) =>
        //earliestStartTime = math.min(earliestStartTime, System.nanoTime())
        putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        latestEndTime = math.max(System.currentTimeMillis(), latestEndTime)
      case ReceiveTimeout =>
        if (!sent && (earliestStartTime != Long.MaxValue || latestEndTime != Long.MinValue)) {
          context.actorSelection("akka://AK/user/monitor") !
            Tuple2(earliestStartTime, latestEndTime)
          sent = true
        }
    }
  }

  val actors = new mutable.HashMap[Int, Array[ActorRef]]

  for (partitionId <- 0 until partitioner.numPartitions) {
    val actorNum = math.pow(2, 32 - PartitionedHTreeMap.BUCKET_LENGTH).toInt
    actors.put(partitionId, new Array[ActorRef](actorNum))
    for (segmentId <- 0 until actorNum) {
      actors(partitionId)(segmentId) = ActorBasedPartitionedHTreeMap.actorSystem.actorOf(
          Props(new WriterActor(partitionId, segmentId)))
    }
  }

  //wrapper of putInner which is called inside the actor to update the state
  private def putExecuteByActor(
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
    if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
      //if MainTable
      partition = math.abs(partition)
    }
    val segmentId = h >>> PartitionedHTreeMap.BUCKET_LENGTH
    if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
      actors(partition)(segmentId) ! ValueAndHash(value.asInstanceOf[SparseVector], h)
    } else {
      actors(partition)(segmentId) ! KeyAndHash(key.asInstanceOf[Int], h)
    }
    value
  }
}

final case class ValueAndHash(vector: SparseVector, hash: Int)
final case class KeyAndHash(vectorId: Int, hash: Int)

object ActorBasedPartitionedHTreeMap {
  var actorSystem: ActorSystem = null
  var tableNum: Int = 0

  var histogramOfSegments: Array[Array[Array[Int]]] = null
  var histogramOfPartitions: Array[Array[Int]] = null
}
