package cpslab.lsh

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import cpslab.lsh.vector.{SparseVector, Vectors}

/**
 * class representing the hash family utilizing the precalculated trick to reduce the computational
 * overhead
 *
 * http://dl.acm.org/citation.cfm?id=1109690&CFID=493748797&CFTOKEN=22809672
 *
 * @param familySize total number of functions in this family
 * @param vectorDim the vector dimensionality supported by this family
 * @param pStableMu the mu value of the Gaussian distribution
 * @param pStableSigma the sigma value of Gaussian distribution
 * @param w W selected
 * @param chainLength the length of the hash function chain
 */
class PrecalculatedHashFamily(
    familySize: Int,
    vectorDim: Int,
    pStableMu: Double,
    pStableSigma: Double,
    w: Int,
    chainLength: Int)
  extends LSHHashFamily[PrecalculatedParameterSet] {

  require(chainLength % 2 == 0,
    "PrecalculatedHashFamily requires hash funciton chain length to be even number")

  // TODO: make it type-parameterized
  private val underlyingHashFamily =
    new PStableHashFamily(familySize, vectorDim, pStableMu, pStableSigma, w, chainLength / 2)

  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family
   * @return the list of LSHTableHashChain
   */
  override def pick(tableNum: Int): List[LSHTableHashChain[PrecalculatedParameterSet]] = {
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val underlyingFunctions = underlyingHashFamily.pick(familySize)
    val generatedHashChains = new Array[LSHTableHashChain[PrecalculatedParameterSet]](tableNum)
    // generate the hash chain
    for (i <- 0 until tableNum) {
      val hashFunctionIndices = (0 until 2).map(_ => uniformRandomizer.nextInt(familySize))
      val hashChain = List(PrecalculatedParameterSet(hashFunctionIndices(0)),
        PrecalculatedParameterSet(hashFunctionIndices(1)))
      generatedHashChains(i) = new PrecalculatedHashChain(underlyingFunctions, chainLength,
        hashChain)
    }
    generatedHashChains.toList
  }

  /**
   * generate pStable hash functions from file; called by generateTableChainFromFile
   * @param filePath path of the file storing pstable hash function parameters
   * @param hashTableNum the number of the hash tables
   * @return the list of p-stable hash chain, essentially they are u functions, each of which
   *         contains k /2 hashes. (k is the length of the index on each table)
   */
  private def generatePStableHashChainsFromFile(filePath: String, hashTableNum: Int):
      List[PStableHashChain] = {
    val underlyingHashesList = new ListBuffer[PStableParameterSet]
    // generate all pstable hash functions
    for (line <- Source.fromFile(filePath).getLines()) {
      val Array(vectorString, bInStr, wInStr) = line.split(";")
      val vectorA = Vectors.fromString(vectorString)
      val b = bInStr.toDouble
      val w = wInStr.toInt
      underlyingHashesList += new PStableParameterSet(
        Vectors.sparse(vectorA._1, vectorA._2, vectorA._3).asInstanceOf[SparseVector],
        b, w)
    }
    // the length of each u function chain
    val pStableChainLength = math.sqrt(hashTableNum).toInt
    // u
    val pStableHashFunctionsSet = underlyingHashesList.grouped(pStableChainLength).
      map(_.toList).toList
    pStableHashFunctionsSet.map(pStableParams =>
      new PStableHashChain(pStableParams.size, pStableParams))
  }

  /**
   * generate a hash table chain from the file
   * @param filePaths the path of the files storing the hash chain, if it needs more than one file
   *                  to generate the hash chain, the paths are delimited by ,
   * @param tableNum the number of hash tables
   * @return the list of LSHTableHashChain
   */
  override def generateTableChainFromFile(filePaths: String, tableNum: Int):
      List[LSHTableHashChain[PrecalculatedParameterSet]] = {
    val Array(precalculatedFilePath, pStableFilePath) = filePaths.split(",")
    try {
      // pStable chain number == sqrt(tableNumber)
      val pStableHashChains = generatePStableHashChainsFromFile(pStableFilePath,
        math.sqrt(tableNum).toInt)
      // generate precalculated hash family
      val precalculatedHashFileLines = Source.fromFile(precalculatedFilePath).getLines().toList
      require(precalculatedHashFileLines.length == tableNum, "table number must be equal to " +
        "precalculatedHashFile length")
      val precalculatedChains = new ListBuffer[PrecalculatedHashChain]
      for (line <- precalculatedHashFileLines) {
        val Array(idx1, idx2) = line.split(";").map(_.toInt)
        val chain = new PrecalculatedHashChain(pStableHashChains, chainLength,
          List(PrecalculatedParameterSet(idx1), PrecalculatedParameterSet(idx2)))
        precalculatedChains += chain
      }
      precalculatedChains.toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }
}

/**
 * implementation of a hash chain containing function H(v) = FLOOR((a * v  + b) / W)
 * @param concatenatedChains the hash chain of the precalcuated functions
 * @param chainSize the length of the chain
 * @param chainedFunctions the list of the funcitons used to calculate the index of the vector
 */
//TODO: this class is not supposed to be private[cpslab], instead, we should limit it in lsh
// currently, we only relax the restriction to implement two-level partition in PLSH
private[cpslab] class PrecalculatedHashChain(
    concatenatedChains: List[LSHTableHashChain[PStableParameterSet]],
    chainSize: Int,
    chainedFunctions: List[PrecalculatedParameterSet])
  extends LSHTableHashChain[PrecalculatedParameterSet](chainSize, chainedFunctions) {

  require(chainedFunctions.size == 2)

  def firstPartitionerID = chainedFunctions.head.functionIdx

  def secondPartitionerID = chainedFunctions(1).functionIdx

  def computeFirstLevelIndex(vector:SparseVector): Array[Byte] = {
    concatenatedChains(chainedFunctions(0).functionIdx).compute(vector)
  }

  def computeSecondLevelIndex(vector: SparseVector): Array[Byte] = {
    concatenatedChains(chainedFunctions(1).functionIdx).compute(vector)
  }

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions
   * defined in this class
   * each function generates an integer which is then converted into a byte array and all integers
   * are concatenated as the index of the element in the table
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  override def compute(vector: SparseVector): Array[Byte] = {
    import PrecalculateCache._
    // generate integer typed index
    val indexInATable = chainedFunctions.foldLeft(Array.fill[Byte](0)(0))(
      (existingByteArray, ps) => {
        val newByteArray = {
          // calculate new Byte Array
          // assuming normalized vector
          if (cache.contains(vector.vectorId) && cache(vector.vectorId).contains(ps.functionIdx)) {
            cache(vector.vectorId)(ps.functionIdx)
          } else {
            val pStablePS = concatenatedChains(ps.functionIdx)
            val key = pStablePS.compute(vector)
            cache.getOrElseUpdate(vector.vectorId, new ConcurrentHashMap[Int, Array[Byte]]) +=
              ps.functionIdx -> key
            key
          }
        }
        existingByteArray ++ newByteArray
      })
    // generate byte array typed index
    indexInATable.map(idx => ByteBuffer.allocate(4).putInt(idx).array()).
      foldLeft(Array.fill(0)(0.toByte))((existingByteArray, newByteArray) =>
      existingByteArray ++ newByteArray)
  }
}

private object PrecalculateCache {
  //vectorId -> (underlying hash function id -> value)
  val cache = new ConcurrentHashMap[Int, ConcurrentHashMap[Int, Array[Byte]]]
}

/**
 * parameter set for the precalculated hash family;
 * @param functionIdx the index of the hash function; the function this index referring to is
 *                    supposed to be with the length of k / 2, where k is the total number of hash
 *                    functions in each hash table;
 */
private[cpslab] case class PrecalculatedParameterSet(functionIdx: Int)
  extends LSHFunctionParameterSet
