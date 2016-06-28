package cpslab.lsh

import cpslab.db.Serializer
import cpslab.lsh.vector.SparseVector

trait Hasher {
  def hash[K](key: K, keySerializer: Serializer[K]): Int
}


class DefaultHasher(hashSalt: Int) extends Hasher {

  override def hash[K](key: K, keySerializer: Serializer[K]): Int = key match {
    case intKey: Int =>
      System.out.println("hashing " + key + " as " + intKey)
      intKey
    case x =>
      key.hashCode()
    //TODO investigate if hashSalt has any effect
    /*var h: Int = keySerializer.hashCode(key) ^ hashSalt
    //stear hashcode a bit, to make sure bits are spread
    h = h * -1640531527
    h = h ^ h >> 16
    //TODO koloboke credit
    h*/
  }
}

class LocalitySensitiveHasher(lsh: LSH, tableId: Int) extends Hasher {
  assert(lsh != null)
  override def hash[K](key: K, keySerializer: Serializer[K]): Int = {
    lsh.calculateIndex(key.asInstanceOf[SparseVector], tableId)(0)
  }
}


