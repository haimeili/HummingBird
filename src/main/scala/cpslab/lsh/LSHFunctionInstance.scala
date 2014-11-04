package cpslab.lsh

import org.apache.spark.mllib.linalg.SparseVector

import scala.collection.mutable.{HashMap, ListBuffer}

trait LSH {
  // chainNum => LSUFunctionInstance
  private[lsh] val hashFunctionChains = new HashMap[Int, ListBuffer[LSHFunctionInstance]]

  /**
   * insert new data to the hashTables
   * @param newVector the vector to be inserted
   * @return the hashtable id and the position this newVector is inserted to
   */
  def insertData(newVector: SparseVector): (Int, Int)

  /**
   * query the data
   * @param query the query vector
   * @return the similar vectors
   */
  def queryData(query: SparseVector): Array[SparseVector]
}

trait LSHFunctionInstance {
  def compute(input: SparseVector): Double
}
