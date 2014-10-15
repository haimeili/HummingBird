package cpslab.lsh.base

import cpslab.util.Configuration
import org.scalatest.{BeforeAndAfter, FunSuite}

import cpslab.lsh.{LSHType, LSHFactory}

class LSHBaseFactorySuite extends FunSuite with BeforeAndAfter {

  test("familySize must be larger than chainLength, otherwise should throw an exception") {
    intercept[IllegalArgumentException] {
      LSHFactory(LSHType.BASE,
        new Configuration(getClass.getClassLoader.getResource("buggybase.conf").getFile))
    }
  }

  test("LSHBaseFactory can generate LSHBase instances correctly") {
    val config = new Configuration(getClass.getClassLoader.getResource("base.conf").getFile)
    val baseFactory = LSHFactory(LSHType.BASE, config)
    val baseLSH = baseFactory.newInstance()
    assert(baseLSH.hashFunctionChains.size === config.getInt("cpslab.lshquery.lsh.chainNum"))
    for ((id, functions) <- baseLSH.hashFunctionChains) {
      assert(functions.size === config.getInt("cpslab.lshquery.lsh.chainLength"))
    }
  }
}
