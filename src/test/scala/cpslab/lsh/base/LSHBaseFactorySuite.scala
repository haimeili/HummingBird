package cpslab.lsh.base

import cpslab.lsh.LSHFactory
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfter, FunSuite}

class LSHBaseFactorySuite extends FunSuite with BeforeAndAfter {

  test("familySize must be larger than chainLength, otherwise should throw an exception") {
    intercept[IllegalArgumentException] {
      LSHFactory(new Configuration(getClass.getClassLoader.getResource("buggybase.conf").getFile))
    }
  }

  test("LSHBaseFactory can generate LSHBase instances correctly") {
    val config = new Configuration(getClass.getClassLoader.getResource("base.conf").getFile)
    val baseFactory = LSHFactory(config)
    val baseLSH = baseFactory.newInstance()
    assert(baseLSH.hashFunctionChains.size === config.getInt("cpslab.lshquery.lsh.chainNum"))
    assert(baseLSH.hashTables.size === config.getInt("cpslab.lshquery.lsh.chainNum"))
    for ((id, functions) <- baseLSH.hashFunctionChains) {
      assert(functions.size === config.getInt("cpslab.lshquery.lsh.chainLength"))
    }
  }

  test("same vectors are inserted in the same bucket of LSHBase") {
    val config = new Configuration(getClass.getClassLoader.getResource("base.conf").getFile)
    val baseFactory = LSHFactory(config)
    val baseLSH = baseFactory.newInstance()
    val sparseVectorArray = new Array[(Int, Double)](3)
    sparseVectorArray(0) = (0, 0.1)
    sparseVectorArray(1) = (1, 0.1)
    sparseVectorArray(2) = (2, 0.1)
    val sparseVector = Vectors.sparse(3, sparseVectorArray).asInstanceOf[SparseVector]
    baseLSH.insertData(sparseVector)
    baseLSH.insertData(sparseVector)
    for ((tableIdx, table) <- baseLSH.hashTables) {
      assert(table.count{ case (index, vectors) => vectors.size == 2} === 1)
      assert(table.count{ case (index, vectors) => vectors.size == 0} === table.size - 1)
    }
  }

  test("query data from LSHBase with deduplication") {
    val config = new Configuration(getClass.getClassLoader.getResource("base.conf").getFile)
    val baseFactory = LSHFactory(config)
    val baseLSH = baseFactory.newInstance()
    val sparseVectorArray = new Array[(Int, Double)](3)
    sparseVectorArray(0) = (0, 0.1)
    sparseVectorArray(1) = (1, 0.1)
    sparseVectorArray(2) = (2, 0.1)
    val sparseVector = Vectors.sparse(3, sparseVectorArray).asInstanceOf[SparseVector]
    baseLSH.insertData(sparseVector)
    baseLSH.insertData(sparseVector)
    val similarVectors = baseLSH.queryData(sparseVector)
    assert(similarVectors.size === 2 * config.getInt("cpslab.lshquery.lsh.chainNum"))
    for (i <- 0 until similarVectors.size) {
      assert(similarVectors(i) === sparseVector)
    }
  }
}
