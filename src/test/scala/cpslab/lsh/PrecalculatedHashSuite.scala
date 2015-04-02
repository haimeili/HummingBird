package cpslab.lsh

import org.scalatest.FunSuite

class PrecalculatedHashSuite extends FunSuite {

  test("PrecalculatedHashChain calculates the index correctly for single hash function") {
    val hashFamily = new PrecalculatedHashFamily(familySize = 10, vectorDim = 3, pStableMu = 0,
      pStableSigma = 0.5, w = 3, chainLength = 2)
    val hashTableNum = 100
    val generatedHashChain = hashFamily.pick(hashTableNum)
    assert(generatedHashChain.size === hashTableNum)
    for (hashChain <- generatedHashChain) {
      assert(hashChain.chainLength === 2)
    }
  }


  test("Precalculated HashFamily generates pStableParameterSet from file correctly") {
    val hashFamily = new PrecalculatedHashFamily(familySize = 0, vectorDim = 3, pStableMu = 0,
      pStableSigma = 0.5, w = 0, chainLength = 4)
    val hashChains = hashFamily.generateTableChainFromFile(
      s"${getClass.getClassLoader.getResource("testprecalculated").getFile}," +
        s"${getClass.getClassLoader.getResource("testprecalculated_pstable").getFile}",
      tableNum = 9)
    assert(hashChains.size === 9)
    for (hashChain <- hashChains) {
      assert(hashChain.chainedHashFunctions.length === 2)
    }
    val functionIndexArray = hashChains.flatMap(_.chainedHashFunctions.map(_.functionIdx))
    assert(functionIndexArray === List(0, 2, 0, 1, 0, 0, 1, 2, 1, 0, 1, 1, 2, 1, 2, 0, 2, 2) )
  }
}
