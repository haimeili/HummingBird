package cpslab.lsh

import java.nio.ByteBuffer

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import breeze.stats.distributions.Gaussian
import cpslab.lsh.vector.{SparseVector, Vectors}

/**
 * a hash family containing functions H(v) = FLOOR((a * v  + b) / W)
 * vector a is generated according to the p-stable distribution (Gaussian Distribution)
 * @param familySize total number of functions in this family 
 * @param vectorDim the vector dimensionality supported by this family 
 * @param pStableMu the mu value of the Gaussian distribution
 * @param pStableSigma the sigma value of Gaussian distribution
 * @param w W selected
 * @param chainLength the length of the hash function chain
 */
private[lsh] class PStableHashFamily(
    familySize: Int, 
    vectorDim: Int, 
    pStableMu: Double, 
    pStableSigma: Double,
    w: Int, 
    chainLength: Int) extends LSHHashFamily[PStableParameterSet] {
  
  /**
   * initialize the hash family
   * @return the Array containing all hash functions in this family 
   */
  private[lsh] def initHashFamily: Array[PStableParameterSet] = {
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val hashFamily = new Array[PStableParameterSet](familySize)
    val gaussianDist = new Gaussian(pStableMu, pStableSigma)
    // initialize hashFamily
    for (i <- 0 until familySize) {
      // step 1: generate vector a
      val vectorADimValues = (0 until vectorDim).map(idx => (idx, gaussianDist.sample()))
      val nonZeroIdx = vectorADimValues.filter(_._2 != 0).map(_._1).toArray
      val nonZeroValues = vectorADimValues.filter(_._2 != 0).map(_._2).toArray
      val vectorA = new SparseVector(vectorDim, nonZeroIdx, nonZeroValues)
      // step 2: select b
      val b = uniformRandomizer.nextInt(w)
      // step 3: generate each hash function chain
      hashFamily(i) = new PStableParameterSet(vectorA, w, b)
    }
    hashFamily
  }
  
  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family  
   * @return the list of LSHTableHashChain
   */
  override def pick(tableNum: Int): List[LSHTableHashChain[PStableParameterSet]] = {
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val hashFamily = initHashFamily
    val generatedHashChains = new Array[LSHTableHashChain[PStableParameterSet]](tableNum)

    // generate the hash chain
    for (i <- 0 until tableNum) {
      val hashFunctionChain = (0 until chainLength).map(_ => 
        hashFamily(uniformRandomizer.nextInt(familySize))).toList
      generatedHashChains(i) = new PStableHashChain(chainLength, hashFunctionChain)
    }
    generatedHashChains.toList
  }

  /**
   * generate a hash table chain from the file
   * assumed file format (for each line)
   * vector A; b; w
   * @param filePath the path of the file storing the hash chain
   * @param tableNum the number of hash tables                 
   * @return the list of LSHTableHashChain
   */
  override def generateTableChainFromFile(filePath: String, tableNum: Int): 
  List[LSHTableHashChain[PStableParameterSet]] = {
    val paraSetList = new ListBuffer[PStableParameterSet]
    try {
      for (line <- Source.fromFile(filePath).getLines()) {
        val Array(vectorString, bInStr, wInStr) = line.split(";")
        val vectorA = Vectors.fromString(vectorString)
        val b = bInStr.toDouble
        val w = wInStr.toInt
        paraSetList += new PStableParameterSet(
          Vectors.sparse(vectorA._1, vectorA._2, vectorA._3).asInstanceOf[SparseVector],
          b, w)
      }
      val groupedParaSets = paraSetList.grouped(chainLength)
      groupedParaSets.map(paraSet => new PStableHashChain(chainLength, paraSet.toList)).toList
    } catch {
      case e: Exception => 
        e.printStackTrace()
        null
    }
  }
}

/**
 * implementation of a hash chain containing function H(v) = FLOOR((a * v  + b) / W)
 * @param chainSize the length of the chain
 * @param chainedFunctions the list of the funcitons used to calculate the index of the vector
 */
private[lsh] class PStableHashChain(chainSize: Int, chainedFunctions: List[PStableParameterSet]) 
  extends LSHTableHashChain[PStableParameterSet](chainSize, chainedFunctions) {
  
  require(chainSize == chainedFunctions.size)
  
  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions
   * defined in this class
   * each function generates an integer which is then converted into a byte array and all integers
   * are concatenated as the index of the element in the table
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  override def compute(vector: SparseVector): Array[Byte] = {
    // generate integer typed index
    val indexInATable = chainedFunctions.foldLeft(Array.fill(0)(0))((existingByteArray, ps2) => {
      val newByteArray = {
        // calculate new Byte Array
        // assuming normalized vector
        // TODO: optimize the efficiency with bit vector
        var sum = 0.0
        for (idx <- vector.indices) {
          if (ps2.a.indices.contains(idx)) {
            sum += ps2.a.values(ps2.a.indices.indexOf(idx)) * 
              vector.values(vector.indices.indexOf(idx))      
          }
        }
        Array(((sum + ps2.b) / ps2.w).toInt)
      }
      existingByteArray ++ newByteArray
    })
    // generate byte array typed index
    indexInATable.map(idx => ByteBuffer.allocate(4).putInt(idx).array()).
      foldLeft(Array.fill(0)(0.toByte))((existingByteArray, newByteArray) => 
      existingByteArray ++ newByteArray
    )
  }
}

/**
 * This parameter set forms the hash function 
 * 
 * H(v) = FLOOR((a * v  + b) / W) 
 * @param a a is a d-dimensional random vector with entries chosen independently from a p-stable 
 *          distribution
 * @param b b is a real number chose uniformly from [0, W]
 * @param w W is an integer which should be large enough
 *
 */
private[lsh] class PStableParameterSet(val a: SparseVector, val b: Double, val w: Int)
  extends LSHFunctionParameterSet {
  
  override def toString: String = s"$a;$b;$w"
}


