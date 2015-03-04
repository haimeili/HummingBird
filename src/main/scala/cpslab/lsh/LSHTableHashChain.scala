package cpslab.lsh

import cpslab.vector.SparseVector

/**
 * the class implementing the functions chaining in one of the hash tables
 * @param chainLength the number of hash functions
 * @param chainIndexCalculator the parameter setup for one of the functions
 * @tparam T the definition of the parameters set
 */
private[lsh] abstract class LSHTableHashChain[+T <: LSHFunctionParameterSet](
    private[lsh] val chainLength: Int,
    chainIndexCalculator: List[T]) {

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions 
   * defined in this class
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  def compute(vector: SparseVector): Array[Byte]
  
}
