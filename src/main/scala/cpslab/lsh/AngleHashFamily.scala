package cpslab.lsh

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}

private[lsh] class AngleHashFamily(
    familySize: Int,
    vectorDim: Int,
    chainLength: Int) extends LSHHashFamily[AngleParameterSet] {

  private def getNewUnitVector: SparseVector = {
    val values = {
      val arr = (for (vectorDim <- 0 until vectorDim) yield Random.nextDouble()).toArray
      arr.map(value => if (Random.nextInt(2) > 0) value else -1 * value)
    }
    val indices = values.zipWithIndex.filter{case (value, index) => value != 0}.map(_._2)
    //normailization
    val sqrSum = math.sqrt(
      values.foldLeft(0.0){case (currentSum, newNum) => currentSum + newNum * newNum})
    new SparseVector(Vectors.nextVectorID, indices.length, indices, values.map( _ / sqrSum))
  }

  private[lsh] def initHashFamily: Array[AngleParameterSet] = {
    val parameters = new ListBuffer[AngleParameterSet]
    for (i <- 0 until familySize) {
      parameters += AngleParameterSet(getNewUnitVector)
    }
    parameters.toArray
  }

  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family
   * @return the list of LSHTableHashChain
   */
  override def pick(tableNum: Int): List[LSHTableHashChain[AngleParameterSet]] = {
    val hashFamily = initHashFamily
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val generatedHashChains = new Array[LSHTableHashChain[AngleParameterSet]](tableNum)
    for (tableId <- 0 until tableNum) {
      val hashFunctionChain = (0 until chainLength).map(_ =>
        hashFamily(uniformRandomizer.nextInt(familySize))).toList
      generatedHashChains(tableId) = new AngleHashChain(chainLength, hashFunctionChain)
    }
    generatedHashChains.toList
  }

  /**
   * generate a hash table chain from the file
   * @param filePath the path of the file storing the hash chain
   * @param tableNum the number of hash tables*
   * @return the list of LSHTableHashChain
   */
  override def generateTableChainFromFile(filePath: String, tableNum: Int):
      List[LSHTableHashChain[AngleParameterSet]] = {
    val paraSetList = new ListBuffer[AngleParameterSet]
    try {
      for (vectorString <- Source.fromFile(filePath).getLines()) {
        val unitVector = Vectors.fromString(vectorString)
        paraSetList += new AngleParameterSet(
          Vectors.sparse(unitVector._4, unitVector._1, unitVector._2, unitVector._3).
            asInstanceOf[SparseVector])
      }
      val groupedParaSets = paraSetList.grouped(chainLength)
      groupedParaSets.map(paraSet => new AngleHashChain(chainLength, paraSet.toList)).toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }
}

private[lsh] class AngleHashChain(chainSize: Int, chainedFunctions: List[AngleParameterSet])
  extends LSHTableHashChain[AngleParameterSet](chainSize, chainedFunctions) {

  private def sign(input: Double): Int = if (math.acos(input) <= 0) 0 else 1

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions
   * defined in this class
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  override def compute(vector: SparseVector): Int = {
    var result = 0
    for (hashFunctionId <- 0 until chainSize) {
      val signResult = sign(
        SimilarityCalculator.fastCalculateSimilarity(chainedFunctions(hashFunctionId).a,
        vector))
      result = result << 1 | signResult
    }
    result
  }
}

private[lsh] case class AngleParameterSet(a: SparseVector) extends LSHFunctionParameterSet {

  override def toString: String = a.toString
}






