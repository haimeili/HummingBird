package cpslab.lsh.vector

import java.util

import scala.collection.mutable

private[cpslab] object SimilarityCalculator {
  //assuming the normalized vectors
  def calculateSimilarity(vector1: SparseVectorWrapper, vector2: SparseVectorWrapper): Double = {
    val sparseVector1 = vector1.sparseVector
    val sparseVector2 = vector2.sparseVector
    calculateSimilarity(
      new SparseVector(sparseVector1.vectorId,
        sparseVector1.size, sparseVector1.indices, sparseVector1.values),
      new SparseVector(sparseVector2.vectorId,
        sparseVector2.size, sparseVector2.indices, sparseVector2.values))
  }

  def calculateSimilarity(vector1: SparseVector, vector2: SparseVector): Double = {
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val vector1Map = new mutable.HashMap[Int, Double]
    val vector2Map = new mutable.HashMap[Int, Double]
    for (i <- 0 until vector1.indices.size) {
      vector1Map += vector1.indices(i) -> vector1.values(i)
    }
    for (i <- 0 until vector2.indices.size) {
      vector2Map += vector2.indices(i) -> vector2.values(i)
    }
    for ((idx, value) <- vector1Map) {
      similarity += {
        if (vector2Map.contains(idx)) {
          value * vector2Map(idx)
        } else {
          0.0
        }
      }
    }
    similarity
  }
  
  def fastCalculateSimilarity(vector1: SparseVector, vector2: SparseVector): Double = {
    require(vector1 != null  && vector2 != null)
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val validBits = vector1.bitVector.clone().asInstanceOf[util.BitSet]
    validBits.and(vector2.bitVector)
    var nextSetBit = validBits.nextSetBit(0)
    while (nextSetBit != -1) {
      similarity += vector1.indexToMap(nextSetBit) * vector2.indexToMap(nextSetBit)
      nextSetBit = validBits.nextSetBit(nextSetBit + 1)
    }
    similarity
  }
}
