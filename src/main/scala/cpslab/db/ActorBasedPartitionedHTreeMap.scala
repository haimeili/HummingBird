package cpslab.db

import java.util.concurrent.ExecutorService

import akka.actor.{Actor, Props, ActorSystem}
import com.typesafe.config.Config
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
  extends PartitionedHTreeMap[K, V](
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
    ramThreshold){

  class WriterActor(partitionId: Int) extends Actor {
    override def receive: Receive = {
      case (vector: SparseVector, h: Int) =>
        putExecuteByActor(partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
      case (vectorId: Int, h: Int) =>
        putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
    }
  }


  val actorSystem = ActorSystem(name, conf)

  val actors =
    for (i <- -partitioner.numPartitions + 1 until partitioner.numPartitions)
      yield actorSystem.actorOf(Props(new WriterActor(i)))

  private def putExecuteByActor(
    partition: Int,
    h: Int,
    key: K,
    value: V): Unit = {
    initPartitionIfNecessary(partition)
    var ret: V = value
    try {
      //partitionRamLock.get(partition).writeLock.lock
      ret = putInner(key, value, h, partition)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      //partitionRamLock.get(partition).writeLock.unlock
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
        if (hasher.isInstanceOf[LocalitySensitiveHasher])
          h
        else
          key
      ).asInstanceOf[K])
    if (valueSerializer == null) {
      actors(partition) ! Tuple2(key, h)
    } else {
      actors(partition) ! Tuple2(value, h)
    }
    value
  }
}
