package cpslab.lsh

import cpslab.vector.SparseVector

/**
 * Implementation class of LSH 
 * This class wraps one or more LSHTableHashChains
 * By passing different lshFamilyName, we can implement different lsh schema
 */
private[cpslab] class LSH(tableNum: Int, lshFamilyName: String) extends Serializable {
  
  // the child classes use the tableIndexGenerators to calculate the index of the element in each 
  // table
  private val tableIndexGenerators: List[LSHTableHashChain[_]] = init()
  
  private def init[T <: LSHFunctionParameterSet](): List[LSHTableHashChain[_]] = {
    val lshFamily: Option[LSHHashFamily[T]] = lshFamilyName match {
      case x => None
    }
    require(lshFamily.isDefined)
    lshFamily.map(lshHashFamily => lshHashFamily.pick(tableNum)).get
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