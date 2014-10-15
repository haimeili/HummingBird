package cpslab.lsh

import org.apache.spark.mllib.linalg.SparseVector

import scala.collection.mutable.{ListBuffer, HashMap}

trait LSH {
  private[lsh] val hashFunctionChains = new HashMap[Int, ListBuffer[LSHFunctionInstance]]
}

trait LSHFunctionInstance {
  def compute(input: SparseVector): Double
}
