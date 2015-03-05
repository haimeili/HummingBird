package cpslab.lsh

import com.typesafe.config.{Config, ConfigFactory}
import cpslab.vector.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class LSHSuite extends FunSuite with BeforeAndAfterAll {

  var config: Config = _

  override def beforeAll() {
    config = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.familySize = 10
         |cpslab.lsh.tableNum = 100
         |cpslab.lsh.vectorDim = 1024
         |cpslab.lsh.chainLength = 2
         |cpslab.lsh.family.pstable.mu = 0.0
         |cpslab.lsh.family.pstable.sigma = 0.02
         |cpslab.lsh.family.pstable.w = 3
      """.stripMargin)

  }
  
  test("LSH initialize Hash Family and Hash Chain correctly") {
    val lsh = new LSH("pStable", config)
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val keyArray = lsh.calculateIndex(testVector)
    assert(keyArray.length === 100)
    for (key <- keyArray) {
      assert(key.length === 8)
    }
  }
}
