package cpslab.utils

import scala.reflect.ClassTag

import com.typesafe.config.Config
import cpslab.db.Partitioner
import cpslab.deploy.LSHServer
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

class HashPartitioner[K](numPartitions: Int) extends Partitioner[K](numPartitions) {
  override def getPartition(value: K): Int = {
    value.hashCode() % numPartitions
  }
}

class LocalitySensitivePartitioner[K](conf: Config, tableId: Int, partitionBits: Int)
  extends Partitioner[K](1 << partitionBits) {

  val localitySensitiveHashing = new LSH(conf)

  override def getPartition(hashCode: K): Int = {
    val hashValueInInteger = hashCode.asInstanceOf[Int].hashCode()
    //val partitionId = objHashValue >>> (32 - numBits)
    //partitionId
    //build vector
    val vector = new Array[Int](32)
    for (i <- 0 until 32) {
      vector(i) = (hashValueInInteger & (1 << i)) >>> i
    }
    val index = vector.zipWithIndex.filter(_._1 != 0).map(_._2)
    val values = vector.filter(_ != 0).map(_.toDouble)
    val v = new SparseVector(0, 32, index, values)
    //re locality-sensitive hashing
    localitySensitiveHashing.calculateIndex(v, tableId)(0) >>> (32 - partitionBits)
  }
}
