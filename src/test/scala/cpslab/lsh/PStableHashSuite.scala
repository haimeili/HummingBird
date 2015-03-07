package cpslab.lsh

import java.nio.ByteBuffer

import scala.util.Random

import cpslab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.FunSuite

class PStableHashSuite extends FunSuite {
  
  test("PStableHashChain calculates the index correctly for single hash function") {
    val randomVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet = new PStableParameterSet(randomVector, 10, 3)
    val hashChain = new PStableHashChain(1, List(hashParameterSet))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    assert(ByteBuffer.wrap(hashChain.compute(testVector)).getInt === 4)
  }

  test("PStableHashChain calculates the index correctly for multiple hash functions") {
    val randomVector1 = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val randomVector2 = Vectors.sparse(3, Seq((0, 2.0), (1, 2.0), (2, 2.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet1 = new PStableParameterSet(randomVector1, 10, 3)
    val hashParameterSet2 = new PStableParameterSet(randomVector2, 10, 3)
    val hashChain = new PStableHashChain(2, List(hashParameterSet1, hashParameterSet2))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val indexBytes = hashChain.compute(testVector)
    assert(indexBytes.length === 8)
    assert(ByteBuffer.wrap(hashChain.compute(testVector)).getInt(0) === 4)
    assert(ByteBuffer.wrap(hashChain.compute(testVector)).getInt(4) === 5)
  }
  
  test("Hash Family generates PStableHashChain correctly") {
    val hashFamily = new PStableHashFamily(familySize = 100, vectorDim = 3, pStableMu = 0, 
      pStableSigma = 0.5, w = 3, chainLength = 2)
    val hashTableNum = Random.nextInt(100)
    val generatedHashChain = hashFamily.pick(hashTableNum)
    assert(generatedHashChain.size === hashTableNum)
    for (hashChain <- generatedHashChain) {
      assert(hashChain.chainLength === 2)
    }
  }
}
