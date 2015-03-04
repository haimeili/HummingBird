package cpslab.lsh

import com.typesafe.config.Config
import cpslab.vector.SparseVector

/**
 * Implementation class of LSH 
 * This class wraps one or more LSHTableHashChains
 * By passing different lshFamilyName, we can implement different lsh schema
 */
private[cpslab] class LSH(lshFamilyName: String, conf: Config) extends Serializable {
  
  private val tableIndexGenerators: List[LSHTableHashChain[_]] = initHashChains.get
  
  private def initHashChains[T <: LSHFunctionParameterSet]: Option[List[LSHTableHashChain[_]]] = {
    val familySize = conf.getInt("cpslab.lsh.familySize")
    val vectorDim = conf.getInt("cpslab.lsh.vectorDim")
    val chainLength = conf.getInt("cpslab.lsh.chainLength")
    val initializedChains = lshFamilyName match {
      case "pStable" =>
        val mu = conf.getDouble("cpslab.lsh.family.pstable.mu")
        val sigma = conf.getDouble("cpslab.lsh.family.pstable.sigma")
        val w = conf.getInt("cpslab.lsh.family.pstable.w")
        val family = Some(new PStableHashFamily(familySize = familySize, vectorDim = vectorDim,
          chainLength = chainLength, pStableMu = mu, pStableSigma = sigma, w = w))
        pickUpHashChains(family)
      case x => None
    }
    require(initializedChains.isDefined)
    initializedChains
  }
  
  private def pickUpHashChains[T <: LSHFunctionParameterSet](lshFamily: Option[LSHHashFamily[T]]):
  Option[List[LSHTableHashChain[T]]] = {
    require(lshFamily.isDefined, s"$lshFamilyName is not a valid family name")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    lshFamily.map(lshHashFamily => lshHashFamily.pick(tableNum))
  }
  
  
  /**
   * calculate the index of the vector in tables
   * @param vector the vector to be indexed
   * @param validTableIDs the tables where this vector to be put in. If not passing this parameter
   *                      in, the index of the vector in all tables will be calculated
   * @return the index of the vector in tables, the order corresponds to the validTableIDs parameter
   */
  def calculateIndex(vector: SparseVector, validTableIDs: Seq[Int] = 
    0 until tableIndexGenerators.size): Array[Array[Byte]] = {
    (for (i <- 0 until tableIndexGenerators.size if validTableIDs.contains(i))
      yield tableIndexGenerators(i).compute(vector)).toArray
  }
}