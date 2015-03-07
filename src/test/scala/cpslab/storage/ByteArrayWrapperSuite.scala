package cpslab.storage

import org.scalatest.FunSuite

class ByteArrayWrapperSuite extends FunSuite {

  test("ByteArrayWrapper has the right implementation of hashCode and equals") {
    val baw1 = new ByteArrayWrapper(Array.fill(1)(1.toByte))
    val baw2 = new ByteArrayWrapper(Array.fill(1)(1.toByte))
    val baw3 = new ByteArrayWrapper(Array.fill(1)(2.toByte))
    assert(baw1.hashCode() === baw2.hashCode())
    assert(baw1.equals(baw2) === true)
    assert(baw1.hashCode() != baw3.hashCode())
    assert(baw1.equals(baw3) === false)
    
  }
  
}
