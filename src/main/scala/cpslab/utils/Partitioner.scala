package cpslab.utils

import scala.reflect.ClassTag

import cpslab.db.Partitioner

class HashPartitioner[K](numPartitions: Int) extends Partitioner[K](numPartitions) {
  override def getPartition(value: K): Int = {
    value.hashCode() % numPartitions
  }
}

class RangePartitioner[K : Ordering : ClassTag](numPartitions: Int)
  extends Partitioner[K](numPartitions) {

  private var ordering = implicitly[Ordering[K]]

  override def getPartition(value: K): Int = {
    //TODO: finish the range partitioner
    0
  }
}
