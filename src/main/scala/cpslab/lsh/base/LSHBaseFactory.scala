package cpslab.lsh.base

import cpslab.lsh.{LSH, LSHFactory, LSHFunctionInstance}
import cpslab.util.Configuration
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.mllib.linalg.{SparseVector, Vectors}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

// the definition of this lsh is
// floor((a.v + b)/w) where a is a theta dimensional random vector
// with chosen entries following an s-stable distribution, b is a
// real number chosen uniformly from the range [0, w) and w is a large
// constant
class LSHBase extends LSH

class LSHBaseInstance(private val a: SparseVector,
              private val b: Double,
              private val w: Double) extends LSHFunctionInstance {

  override def compute(input: SparseVector): Double = {
    val av = {
      var sum = 0.0
      for (nonZeroIdx <- a.indices) {
        sum += a.values(nonZeroIdx) * input.values(nonZeroIdx)
      }
      sum
    }
    (av + b) / b
  }
}

class LSHBaseFactory(config: Configuration) extends LSHFactory {
  private val w = config.getDouble("cpslab.lshquery.lsh.base.w")
  // TODO: enable configure different distribution
  private val mean = config.getDouble("cpslab.lshquery.lsh.base.mean")
  private val sd = config.getDouble("cpslab.lshquery.lsh.base.sd")
  private val numGenerator = new NormalDistribution(mean, sd)

  override def newInstance(): LSH =  {
    val hashFamily = new Array[LSHBaseInstance](LSHFactory.familySize)
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
      hashFamily(i) = new LSHBaseInstance(randomVector.asInstanceOf[SparseVector], b, w)
    }
    // 2. generate the chains
    val random = new Random(System.currentTimeMillis())
    val lshInstance = new LSHBase
    for (i <- 0 until LSHFactory.chainNum; j <- 0 until LSHFactory.chainLength) {
      lshInstance.hashFunctionChains.getOrElseUpdate(i,
        new ListBuffer[LSHFunctionInstance]) += hashFamily(
          random.nextInt(LSHFactory.familySize))
    }
    lshInstance
  }
}
