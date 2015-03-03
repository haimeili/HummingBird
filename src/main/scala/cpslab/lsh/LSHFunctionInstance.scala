package cpslab.lsh

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}

import cpslab.vector.SparseVector

trait LSH {
  // chainNum => LSUFunctionInstance
  private[lsh] val hashFunctionChains = new HashMap[Int, ListBuffer[LSHFunctionInstance]]
  private[lsh] val hashTables = new mutable.HashMap[Int,
    mutable.HashMap[List[Double], ListBuffer[SparseVector]]]

  /**
   * insert new data to the hashTables
   * @param newVector the vector to be inserted
   */
  def insertData(newVector: SparseVector)

  /**
   * query the data
   * @param query the query vector
   * @return the similar vectors
   */
  def queryData(query: SparseVector): List[SparseVector]
}

trait LSHFunctionInstance {
  def compute(input: SparseVector): Double
}
