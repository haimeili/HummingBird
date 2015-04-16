package cpslab.lsh

import scala.util.Random

import cpslab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.FunSuite

class AngleHashSuite extends FunSuite {

  test("AngleHashChain calculates the index correctly for single hash function") {
    val randomVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet = new AngleParameterSet(randomVector)
    val hashChain = new AngleHashChain(1, List(hashParameterSet))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).asInstanceOf[SparseVector]
    assert(hashChain.compute(testVector) === 1)
  }

  test("AngleHashChain calculates the index correctly for multiple hash function") {
    val randomVector1 = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val randomVector2 = Vectors.sparse(3, Seq((0, 2.0), (1, 2.0), (2, 2.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet1 = new AngleParameterSet(randomVector1)
    val hashParameterSet2 = new AngleParameterSet(randomVector2)
    val hashChain = new AngleHashChain(2, List(hashParameterSet1, hashParameterSet2))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val indexBytes = hashChain.compute(testVector)
    assert(indexBytes === 3)
  }

  test("Hash Family generates AngleHashChain correctly") {
    val hashFamily = new AngleHashFamily(familySize = 100, vectorDim = 3, chainLength = 2)
    val hashTableNum = Random.nextInt(100)
    val generatedHashChain = hashFamily.pick(hashTableNum)
    assert(generatedHashChain.size === hashTableNum)
    for (hashChain <- generatedHashChain) {
      assert(hashChain.chainLength === 2)
    }
  }

  test("Angle HashFamily generates AngleParameterSet from file correctly") {
    val hashFamily = new AngleHashFamily(familySize = 0, vectorDim = 3, chainLength = 3)
    val hashChain = hashFamily.generateTableChainFromFile(
      getClass.getClassLoader.getResource("testanglefile").getFile, 3)
    assert(hashChain.size === 3)
    val firstChain = hashChain(0)
    assert(firstChain.chainedHashFunctions.size === 3)
    for (para <- firstChain.chainedHashFunctions) {
      assert(para.a.toString === "(1,3,[0,1],[1.0,2.0])")
    }
    val secondChain = hashChain(1)
    assert(secondChain.chainedHashFunctions.size === 3)
    for (para <- secondChain.chainedHashFunctions) {
      assert(para.a.toString === "(2,3,[0,1],[1.0,3.0])")
    }
    val thirdChain = hashChain(2)
    assert(thirdChain.chainedHashFunctions.size === 3)
    for (para <- thirdChain.chainedHashFunctions) {
      assert(para.a.toString === "(3,3,[0,1],[1.0,4.0])")
    }
  }
}
