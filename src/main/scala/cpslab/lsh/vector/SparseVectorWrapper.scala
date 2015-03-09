package cpslab.lsh.vector

import akka.contrib.pattern.ShardRegion.ShardId

/**
 * the class represents the vector as well as its ID and the index of bucket which it belongs to on 
 * each table
 * @param vectorID the unique ID of the vector
 * @param bucketIndex the index of the bucket this vector belongs to
 * @param sparseVector the vector data
 */
case class SparseVectorWrapper(vectorID: ShardId, bucketIndex: Array[Array[Byte]], 
    sparseVector: SparseVector)
