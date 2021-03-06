package cpslab.deploy.benchmark

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

class LSHIndex(lsh: LSH) {

  val array = Array.fill[mutable.HashMap[Int, ListBuffer[SparseVector]]](
    lsh.tableIndexGenerators.length)(new mutable.HashMap[Int, ListBuffer[SparseVector]])

  def insert(vector: SparseVector): Unit = {
    val indices = lsh.calculateIndex(vector)
    for (i <- 0 until array.length) {
      array(i).synchronized {
        array(i).getOrElseUpdate(indices(i), new ListBuffer[SparseVector]) += vector
      }
    }
  }

  def query(query: SparseVector): mutable.HashSet[Int] = {
    val indices = lsh.calculateIndex(query)
    val results = new mutable.HashSet[Int]
    for (i <- 0 until array.length) {
      val candidates = array(i).get(indices(i))
    }
    results
  }
}
