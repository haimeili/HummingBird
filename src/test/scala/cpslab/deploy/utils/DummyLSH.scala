package cpslab.deploy.utils

import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

private[deploy] class DummyLSH(conf: Config) extends LSH(conf) {
  override def calculateIndex(vector: SparseVector): Array[Int] = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val index = Array.fill[Int](tableNum)(0)
    // i.toByte ensure that the vector is distributed to all machines
    for (i <- 0 until tableNum) index(i) = i
    index
  }
}
