package cpslab.deploy.benchmark

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}

// an inverted index implementation for sparse vector
class InvertedIndex(dim: Int) {

  val index = Array.fill[ListBuffer[SparseVector]](dim)(new ListBuffer[SparseVector])

  def insert(vector: SparseVector): Unit = {
    for (i <- vector.indices) {
      index(i).synchronized {
        index(i) += vector
      }
    }
  }

  def query(query: SparseVector): mutable.HashSet[Int] = {
    val results = new mutable.HashSet[Int]
    for (i <- query.indices) {
      index(i).synchronized {
        val candidates = index(i)
        for (v <- candidates) {
          val similarity = SimilarityCalculator.calculateSimilarity(query, v)
          if (similarity > 0.9) {
            results += v.vectorId
          }
        }
      }
    }
    results
  }
}
