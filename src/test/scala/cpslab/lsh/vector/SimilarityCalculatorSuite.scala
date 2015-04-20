package cpslab.lsh.vector

import org.scalatest.FunSuite

class SimilarityCalculatorSuite extends FunSuite {
  
  test("fast similarity calculation") {
    val vector1 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0))).asInstanceOf[SparseVector]
    val vector2 = Vectors.sparse(10, Seq((2, 1.0), (3, 1.0))).asInstanceOf[SparseVector]
    val sim = SimilarityCalculator.fastCalculateSimilarity(vector1, vector2)
    assert(sim === 2.0)
    val vector3 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0))).asInstanceOf[SparseVector]
    val vector4 = Vectors.sparse(10, Seq((5, 1.0), (6, 1.0))).asInstanceOf[SparseVector]
    val sim1 = SimilarityCalculator.fastCalculateSimilarity(vector3, vector4)
    assert(sim1 === 0.0)
  }

  test("fast similarity calculation when there is a mis-matched bit in the middle of the vector") {
    val vector3 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0))).asInstanceOf[SparseVector]
    val vector4 = Vectors.sparse(10, Seq((3, 1.0), (6, 1.0), (9, 1.0))).asInstanceOf[SparseVector]
    val sim1 = SimilarityCalculator.fastCalculateSimilarity(vector3, vector4)
    assert(sim1 === 2.0)
  }
}
