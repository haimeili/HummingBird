package cpslab.lsh

import cpslab.db.Serializer
import cpslab.lsh.vector.SparseVector

trait Hasher[K] {
  def hash(key: K, keySerializer: Serializer[K]): Int
}


class DefaultHasher[K](hashSalt: Int) extends Hasher[K] {

  override def hash(key: K, keySerializer: Serializer[K]): Int = {
    //TODO investigate if hashSalt has any effect
    var h: Int = keySerializer.hashCode(key.asInstanceOf[K]) ^ hashSalt
    //stear hashcode a bit, to make sure bits are spread
    h = h * -1640531527
    h = h ^ h >> 16
    //TODO koloboke credit
    h
  }
}

class LocalitySensitiveHasher(lsh: LSH, tableId: Int) extends Hasher[SparseVector] {
  assert(lsh != null)
  override def hash(key: SparseVector, keySerializer: Serializer[SparseVector]): Int = {
    lsh.calculateIndex(key, tableId)(0)
  }
}


