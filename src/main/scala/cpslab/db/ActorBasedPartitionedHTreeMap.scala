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
    ramThreshold) {

  class WriterActor(partitionId: Int) extends Actor {

    var totalTime = 0L

    context.setReceiveTimeout(60000 milliseconds)

    override def receive: Receive = {
      case (vector: SparseVector, h: Int) =>
        val startTime = System.nanoTime()
        putExecuteByActor(partitionId, h, vector.vectorId.asInstanceOf[K], vector.asInstanceOf[V])
        for (i <- 0 until ActorBasedPartitionedHTreeMap.tableNum) {
          vectorDatabase(i).put(vector.vectorId, true)
        }
        val endTime = System.nanoTime()
        totalTime += endTime - startTime
      case (vectorId: Int, h: Int) =>
        val startTime = System.nanoTime()
        putExecuteByActor(partitionId, h, vectorId.asInstanceOf[K], true.asInstanceOf[V])
        val endTime = System.nanoTime()
        totalTime += endTime - startTime
      case ReceiveTimeout =>
        context.actorSelection("akka://AK/user/monitor") ! totalTime
    }
  }

  val actors = new mutable.HashMap[Int, ActorRef]


  for (i <- -partitioner.numPartitions + 1 until partitioner.numPartitions) {
    actors.put(i, ActorBasedPartitionedHTreeMap.actorSystem.actorOf(Props(new WriterActor(i))))
  }

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
    } catch {
      case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
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
        else {
          key
        }
      ).asInstanceOf[K])
    if (!hasher.isInstanceOf[LocalitySensitiveHasher]) {
      actors(partition) ! Tuple2(value, h)
    } else {
      actors(partition) ! Tuple2(key, h)
    }
    value
  }
}

object ActorBasedPartitionedHTreeMap {
  var actorSystem: ActorSystem = null
  var tableNum: Int = 0
}
