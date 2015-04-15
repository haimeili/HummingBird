package cpslab.lsh

import com.typesafe.config.{Config, ConfigFactory}
import cpslab.TestSettings
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class LSHSuite extends FunSuite with BeforeAndAfterAll {

  var config: Config = _

  override def beforeAll() {
    config = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.familySize = 10
         |cpslab.lsh.tableNum = 100
         |cpslab.lsh.vectorDim = 3
         |cpslab.lsh.chainLength = 2
      """.stripMargin).withFallback(TestSettings.testBaseConf)
  }

  test("LSH initialize Hash Family and Hash Chain correctly") {
    val lsh = new LSH(config)
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val keyArray = lsh.calculateIndex(testVector)
    assert(keyArray.length === 100)
  }
}
