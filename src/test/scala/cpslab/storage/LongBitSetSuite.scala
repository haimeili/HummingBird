package cpslab.storage

import org.scalatest.FunSuite

class LongBitSetSuite extends FunSuite {

  test("LongBitSet index data correctly") {
    val l = new LongBitSet
    l.set(1)
    l.set(1024)
    l.set(1025)
    l.set(2048)
    assert(l.mSets.size === 3)
    assert(l.mSets(0).get(1) === true)
    assert(l.mSets(1).get(0) === true)
    assert(l.mSets(1).get(1) === true)
    assert(l.mSets(2).get(0) === true)
  }

  test("and operation is correct") {
    val l1 = new LongBitSet
    val l2 = new LongBitSet
    l1.set(1025L)
    l2.set(1025L)
    val l3 = l1 & l2
    assert(l3.mSets.size === 1)
    assert(l3.mSets(1).get(1) === true)
  }

  test("or operation is correct") {
    val l1 = new LongBitSet
    val l2 = new LongBitSet
    l1.set(1L)
    l1.set(2048L)
    l2.set(1L)
    l2.set(1025L)
    val l3 = l1 | l2
    assert(l3.mSets.size === 3)
    assert(l3.mSets(0).get(1) === true)
    assert(l3.mSets(1).get(1) === true)
    assert(l3.mSets(2).get(0) === true)
  }
}
