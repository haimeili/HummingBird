package cpslab.lsh.base

import cpslab.lsh.{LSH, LSHFactory, LSHFunctionInstance}
import cpslab.util.Configuration
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

// the definition of this lsh is
// floor((a.v + b)/w) where a is a theta dimensional random vector
// with chosen entries following an s-stable distribution, b is a
// real number chosen uniformly from the range [0, w) and w is a large
// constant
class LSHBase extends LSH {

  /**
   * insert new data to the hashTables
   * @param newVector the vector to be inserted
   * @return the hashtable id and the position this newVector is inserted to
   */
  override def insertData(newVector: SparseVector): Unit = {
    var vectorIndex = List[Double]()
    for (hashTableIndex <- hashTables.keySet) {
      vectorIndex = {
        for (function <- hashFunctionChains(hashTableIndex))
          yield function.compute(newVector)
      }.toList
      hashTables(hashTableIndex).getOrElseUpdate(vectorIndex,
        new ListBuffer[SparseVector]) += newVector
    }
  }

  /**
   * query the data
   * @param query the query vector
   * @return the similar vectors
   */
  override def queryData(query: SparseVector): List[SparseVector] = {
    var similarVectors = List[SparseVector]()
    var vectorIndex = List[Double]()
    for (hashTableIndex <- hashTables.keySet) {
      vectorIndex = {
        for (function <- hashFunctionChains(hashTableIndex))
        yield function.compute(query)
      }.toList
      similarVectors = hashTables(hashTableIndex).getOrElse(vectorIndex, List[SparseVector]()) ++:
        similarVectors
    }
    similarVectors
  }
}

class LSHBaseFunctionInstance(private val a: SparseVector,
              private val b: Double,
              private val w: Double) extends LSHFunctionInstance {

  override def compute(input: SparseVector): Double = {
    val av = {
      var sum = 0.0
      for (nonZeroIdx <- a.indices) {
        if (input.indices.contains(nonZeroIdx)) {
          val idx = a.indices.indexOf(nonZeroIdx)
          sum += a.values(idx) * input.values(idx)
        }
      }
      sum
    }
    (av + b) / b
  }
}

class LSHBaseFactory(config: Configuration) extends LSHFactory {
  private val w = Double.MaxValue
  // TODO: enable configure different distribution
  private val mean = config.getDouble("cpslab.lshquery.lsh.base.mean")
  private val sd = config.getDouble("cpslab.lshquery.lsh.base.sd")
  private val numGenerator = new NormalDistribution(mean, sd)

  override def newInstance(): LSH =  {
    val hashFamily = new Array[LSHBaseFunctionInstance](LSHFactory.familySize)
    // 1. generate the whole family
    for (i <- 0 until LSHFactory.familySize) {
      //1.1 generate a vector
      val v = new ArrayBuffer[(Int, Double)](LSHFactory.vectorDimension)
      for (i <- 0 until LSHFactory.vectorDimension) {
        var value = numGenerator.sample()
        while (value == 0) {
          value = numGenerator.sample()
        }
        v += i -> value
      }
      val randomVector = Vectors.sparse(LSHFactory.vectorDimension, v)
      //1.2 generate a number b
      val b = new Random(System.currentTimeMillis()).nextDouble() * w
      //1.3 generate the hashFunction
      hashFamily(i) = new LSHBaseFunctionInstance(randomVector.asInstanceOf[SparseVector], b, w)
    }
    // 2. generate the chains
    val random = new Random(System.currentTimeMillis())
    val lshInstance = new LSHBase
    for (i <- 0 until LSHFactory.chainNum; j <- 0 until LSHFactory.chainLength) {
      lshInstance.hashFunctionChains.getOrElseUpdate(i,
        new ListBuffer[LSHFunctionInstance]) += hashFamily(
          random.nextInt(LSHFactory.familySize))
      lshInstance.hashTables.getOrElseUpdate(i,
        new mutable.HashMap[List[Double], ListBuffer[SparseVector]])
    }
    lshInstance
  }
}
