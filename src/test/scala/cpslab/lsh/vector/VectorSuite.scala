package cpslab.lsh.vector

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.scalatest.FunSuite

class VectorSuite extends FunSuite {

  test("fromString test") {
    val vectorFile = getClass.getClassLoader.getResource("vectorfile").getFile
    val results = new ListBuffer[(Int, Int, Array[Int], Array[Double])]
    for (line <- Source.fromFile(vectorFile).getLines()) {
      results += Vectors.fromString(line)
    }
    // convert to sparse vector and compare
    val sparseVector1 = new SparseVector(results(0)._1, results(0)._2, results(0)._3, results(0)._4)
    val sparseVector2 = new SparseVector(results(1)._1, results(1)._2, results(1)._3, results(1)._4)
    assert(sparseVector1.toString === "(3,3,[0,1,2],[1.0,2.0,3.0])")
    assert(sparseVector2.toString === "(4,3,[0,1,2],[4.0,5.0,6.0])")
  }

}
