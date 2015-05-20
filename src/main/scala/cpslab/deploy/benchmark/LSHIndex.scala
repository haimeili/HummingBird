package cpslab.deploy.benchmark

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}

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
    println("indices of " + query.vectorId + " is " + indices.toList)
    val results = new mutable.HashSet[Int]
    val calculated = new mutable.HashSet[Int]
    for (i <- 0 until array.length) {
      val candidates = array(i).get(indices(i))
      candidates.foreach(l => l.foreach(v => {
        if (!calculated.contains(v.vectorId) &&
            SimilarityCalculator.fastCalculateSimilarity(v, query) > 0.9) {
          results += v.vectorId
        }
        calculated += v.vectorId
      }))
    }
    results
  }
}
