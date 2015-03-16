package cpslab.lsh

/**
 * the set of the parameters defining a hash function
 */
trait LSHFunctionParameterSet extends Serializable

/**
 * the trait defining all hash functions used in a LSH instance
 * By passing different parameter type T, we implement different LSH schema
 * @tparam T the definition of the parameter set specifying a hash function
 */
private[lsh] trait LSHHashFamily[+T <: LSHFunctionParameterSet] {

  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family  
   * @return the list of LSHTableHashChain
   */
  def pick(tableNum: Int): List[LSHTableHashChain[T]]

  /**
   * generate a hash table chain from the file
   * @param filePath the path of the file storing the hash chain
   * @param tableNum the number of hash tables*
   * @return the list of LSHTableHashChain
   */
  def generateTableChainFromFile(filePath: String, tableNum: Int): List[LSHTableHashChain[T]]
}
