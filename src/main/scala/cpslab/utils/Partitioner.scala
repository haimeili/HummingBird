package cpslab.utils

import cpslab.db.Partitioner

class HashPartitioner[K](numPartitions: Int) extends Partitioner[K](numPartitions) {
  override def getPartition(value: K): Int = {
    value.hashCode() % numPartitions
  }
}
