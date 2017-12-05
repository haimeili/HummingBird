package cpslab.lsh.vector

import org.scalatest.FunSuite

class SimilarityCalculatorSuite extends FunSuite {
  
  test("fast similarity calculation") {
    val vector1 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0)))
      .asInstanceOf[SparseVector]
    val vector2 = Vectors.sparse(10, Seq((2, 1.0), (3, 1.0))).asInstanceOf[SparseVector]
    val sim = SimilarityCalculator.fastCalculateSimilarity(vector1, vector2)
    assert(sim === 2.0)
    val vector3 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0)))
      .asInstanceOf[SparseVector]
    val vector4 = Vectors.sparse(10, Seq((5, 1.0), (6, 1.0))).asInstanceOf[SparseVector]
    val sim1 = SimilarityCalculator.fastCalculateSimilarity(vector3, vector4)
    assert(sim1 === 0.0)
  }

  test("fast similarity calculation when there is a mis-matched bit in the middle of the vector") {
    val vector3 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0)))
      .asInstanceOf[SparseVector]
    val vector4 = Vectors.sparse(10, Seq((3, 1.0), (6, 1.0), (9, 1.0))).asInstanceOf[SparseVector]
    val sim1 = SimilarityCalculator.fastCalculateSimilarity(vector3, vector4)
    assert(sim1 === 2.0)
  }

  test("test similarity ") {
    val vector1 = Vectors.fromString("(3385,784,[4,5,6,7,8,9,10,17,18,21,22,23,25,26,27,28,30,31,32,33,35,39,40,48,49],[224.0,149.0,116.0,237.0,253.0,253.0,254.0,254.0,136.0,253.0,254.0,9.0,213.0,254.0,253.0,96.0,186.0,253.0,246.0,51.0,186.0,230.0,253.0,153.0,254.0])")
    val v1 = new SparseVector(vector1._1, vector1._2, vector1._3, vector1._4)
    val lengthV1 = v1.values.foldLeft(0.0)((l, d) => l + d * d)
    val vector2 = Vectors.fromString("(60460,784,[4,5,6,7,8,9,10,13,14,17,18,21,22,23,26,27,28,30,31,32,35,39,40,49],[154.0,154.0,177.0,253.0,250.0,253.0,253.0,62.0,2.0,253.0,253.0,168.0,241.0,26.0,150.0,253.0,124.0,42.0,236.0,123.0,11.0,78.0,253.0,78.0])")
    val v2 = new SparseVector(vector2._1, vector2._2, vector2._3, vector2._4)
    val lengthV2 = v2.values.foldLeft(0.0)((l, d) => l + d * d)
    println(SimilarityCalculator.fastCalculateSimilarity(v1, v2) /
      (math.sqrt(lengthV1) * math.sqrt(lengthV2)))
  }
}
