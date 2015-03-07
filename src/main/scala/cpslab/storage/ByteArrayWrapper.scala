package cpslab.storage

import java.util.Arrays

private[cpslab] class ByteArrayWrapper(val bytes: Array[Byte]) {
  
  override def hashCode(): Int = {
    val ret = Arrays.hashCode(bytes)
    ret
  }
   
  override def equals(other: Any): Boolean = other match {
    case obj: ByteArrayWrapper =>
      Arrays.equals(bytes, obj.bytes)
    case _ => 
      false
  }
}

object ByteArrayWrapper {
  
  def apply(bytes: Array[Byte]): ByteArrayWrapper = new ByteArrayWrapper(bytes)
  
}

