package cpslab.storage

private[cpslab] trait KeyValueEngine {

  /**
   * get the value from the underlying key value storage system
   * @param key query key
   * @return Option of the value (can be none if the key does not exist)
   */
  def get(key: Array[Byte]): Option[Array[Byte]]

  /**
   * write the key value pair to the underlying key value storage system
   * @param key the key of the pair
   * @param value value of the pair
   * @param allowOverwrite default to be true; if true, the method overwrites key-value pair in the 
   *                       database if key is existing, if false, the method returns false
   * @return true -> write successfully; false -> something wrong happened during the write
   */
  def put(key: Array[Byte], value: Array[Byte], allowOverwrite: Boolean = true): Boolean

  /**
   * write a group of key value pairs to the underlying key value storage system *ATOMICALLY*,
   * Atomic means that the semantics of this method is transactional,  
   * @param keyValuePairs the key value pairs to be written to the underlying KV database
   * @return true -> write successfully; false -> something wrong happened during the write
   */
  def write(keyValuePairs: List[(Array[Byte], Array[Byte])]): Boolean
}
