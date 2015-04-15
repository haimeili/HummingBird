package cpslab.lsh

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
    assert(hashChain.compute(testVector) === 923525)
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
    assert(indexBytes === -1803760374)
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

  test("pStableParameterSet generates string correctly") {
    val vectorA = Vectors.sparse(3, Array(0, 1), Array(1.0, 2.0)).asInstanceOf[SparseVector]
    val paraSet = new PStableParameterSet(vectorA, 0.1, 5)
    assert(paraSet.toString === s"$vectorA;0.1;5")
  }
  
  test("pStable HashFamily generates pStableParameterSet from file correctly") {
    val hashFamily = new PStableHashFamily(familySize = 0, vectorDim = 3, pStableMu = 0,
      pStableSigma = 0.5, w = 0, chainLength = 3)
    val hashChain = hashFamily.generateTableChainFromFile(
      getClass.getClassLoader.getResource("testpstablefile").getFile, 3)
    assert(hashChain.size === 3)
    val firstChain = hashChain(0)
    assert(firstChain.chainedHashFunctions.size === 3)
    for (para <- firstChain.chainedHashFunctions) {
      assert(para.a.toString === "(1,3,[0,1],[1.0,2.0])")
      assert(para.b === 0.1)
      assert(para.w === 5)
    }
    val secondChain = hashChain(1)
    assert(secondChain.chainedHashFunctions.size === 3)
    for (para <- secondChain.chainedHashFunctions) {
      assert(para.a.toString === "(2,3,[0,1],[1.0,3.0])")
      assert(para.b === 0.2)
      assert(para.w === 6)
    }
    val thirdChain = hashChain(2)
    assert(thirdChain.chainedHashFunctions.size === 3)
    for (para <- thirdChain.chainedHashFunctions) {
      assert(para.a.toString === "(3,3,[0,1],[1.0,4.0])")
      assert(para.b === 0.3)
      assert(para.w === 7)
    }
      
  }
}
