package cpslab.deploy.utils

import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

private[deploy] class DummyLSH(conf: Config) extends LSH(conf) {
  override def calculateIndex(vector: SparseVector, validTableIDs: Seq[Int]): Array[Array[Byte]] = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    // i.toByte ensure that the vector is distributed to all machines
    (for (i <- 0 until tableNum) yield Array.fill[Byte](1)(i.toByte)).toArray
  }
}
