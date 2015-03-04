package cpslab.storage

private[cpslab] trait CacheEngine {
  
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
   * @return true -> write successfully; false -> something wrong happened during the write
   */
  def put(key: Array[Byte], value: Array[Byte]): Boolean
  
}

/**
 * not-functional implementation of cache engine.
 */
private[cpslab] class EmptyCacheEngine extends CacheEngine {
  /**
   * get the value from the underlying key value storage system
   * @param key query key
   * @return Option of the value (can be none if the key does not exist)
   */
  override def get(key: Array[Byte]): Option[Array[Byte]] = ???

  /**
   * write the key value pair to the underlying key value storage system
   * @param key the key of the pair
   * @param value value of the pair
   * @return true -> write successfully; false -> something wrong happened during the write
   */
  override def put(key: Array[Byte], value: Array[Byte]): Boolean = ???
}
