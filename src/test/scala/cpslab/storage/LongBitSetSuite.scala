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

  test("output all set bits correctly") {
    val l = new LongBitSet
    l.set(1)
    l.set(1024)
    l.set(1025)
    l.set(2048)
    val a = l.allSetBits()
    assert(a.length === 4)
    assert(a(0) === 2048)
    assert(a(1) === 1024)
    assert(a(2) === 1025)
    assert(a(3) === 1)
  }

  test("flip test") {
    val l = new LongBitSet
    l.set(1)
    l.set(1024)
    l.set(1025)
    l.set(2048)
    val x = l.flip()
    x.mSets(0).and(l.mSets(0))
    x.mSets(1).and(l.mSets(1))
    x.mSets(2).and(l.mSets(2))
    assert(x.mSets.size === 3)
    assert(x.mSets(0).length() === 0)
    assert(x.mSets(1).length() === 0)
    assert(x.mSets(2).length() === 0)
  }
}
