package cpslab.lsh.vector

/**
 * the class represents the vector as well as its ID and the index of bucket which it belongs to on 
 * each table
 * @param bucketIndices the index of the bucket this vector belongs to
 * @param sparseVector the vector data
 */
case class SparseVectorWrapper(bucketIndices: Array[Int], sparseVector: SparseVector)
