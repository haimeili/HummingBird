package cpslab.utils

import scala.reflect.ClassTag

import cpslab.db.Partitioner

class HashPartitioner[K](numPartitions: Int) extends Partitioner[K](numPartitions) {
  override def getPartition(value: K): Int = {
    value.hashCode() % numPartitions
  }
}

class HashBitExtractingPartitioner[K](numBits: Int)
  extends Partitioner[K](1 << (numBits + 1)) {

  override def getPartition(value: K): Int = {
    val objHashValue = value.asInstanceOf[Int].hashCode()
    val partitionId = objHashValue >>> (32 - numBits)
    partitionId
  }
}
