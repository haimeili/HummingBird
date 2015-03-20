package cpslab.storage

import java.util

import scala.collection.mutable

import cpslab.storage.LongBitSet._

private[cpslab] class LongBitSet {
  /**
   * Map from a value stored in high bits of a long index to a bit set mapped to the lower bits
   * of an index. Bit sets size should be balanced - not to long (otherwise setting a single bit may
   * waste megabytes of memory) but not too short (otherwise this map will get too big). Update
   * value of {@code VALUE_BITS} for your needs. In most cases it is ok to keep 1M - 64M values in a
   * bit set, so each bit set will occupy 128Kb - 8Mb.
   */
  private[storage] val mSets = new mutable.HashMap[Long, util.BitSet]

  /**
   * Get set index by long index (extract bits VALUE_BITS-63)
   * @param index Long index
   * @return Index of a bit set in the inner map
   */
  private def getSetIndex(index: Long): Long = index >> VALUE_BITS


  /**
   * Get index of a value in a bit set (bits 0-(VALUE_BITS - 1))
   * @param index Long index
   * @return Index of a value in a bit set
   */
  private def getPos(index: Long): Int = (index & VALUE_MASK).toInt

  /**
   * Helper method to get (or create, if necessary) a bit set for a given long index
   * @param index Long index
   * @return A bit set for a given index (always not null)
   */
  private def bitSet(index: Long): util.BitSet = {
    val iIndex = getSetIndex(index)
    mSets.getOrElseUpdate(iIndex, new util.BitSet(1 << VALUE_BITS))
  }

  /**
   * Set a given value for a given index
   * @param index Long index
   */
  def set(index: Long) {
    bitSet(index).set(getPos(index))
  }

  /**
   * Get a value for a given index
   * @param index Long index
   * @return Value associated with a given index
   */
  def get(index: Long): Boolean = {
    val bitSet = mSets.get(getSetIndex(index))
    bitSet.isDefined && bitSet.get.get(getPos(index))
  }

  /**
   * save a bitset to the LongBitSet
   * @param key the index of the bitset
   * @param bitset the bitset object
   */
  def saveBits(key: Long, bitset: util.BitSet): Unit = {
    mSets += key -> bitset
  }

  def | (other: LongBitSet): LongBitSet = {
    val ret = new LongBitSet
    val shorterSet = if (other.mSets.size > this.mSets.size) this.mSets else other.mSets
    val longerSet = if (other.mSets eq shorterSet) this.mSets else other.mSets
    for (key <- shorterSet.keySet) {
      if (longerSet.get(key).isDefined) {
        val bits = longerSet(key).clone().asInstanceOf[util.BitSet]
        bits.or(shorterSet(key))
        if (bits.cardinality() > 0) {
          ret.saveBits(key, bits)
        }
      } else {
        ret.saveBits(key, shorterSet(key))
      }
    }
    for (key <- longerSet.keySet) {
      if (!shorterSet.get(key).isDefined) {
        ret.saveBits(key, longerSet(key))
      }
    }
    ret
  }

  def & (other: LongBitSet): LongBitSet = {
    val ret = new LongBitSet
    val shorterSet = if (other.mSets.size > this.mSets.size) this.mSets else other.mSets
    val longerSet = if (other.mSets eq shorterSet) this.mSets else other.mSets
    for (key <- shorterSet.keySet) {
      if (longerSet.get(key).isDefined) {
        val bits = longerSet(key).clone().asInstanceOf[util.BitSet]
        bits.and(shorterSet(key))
        if (bits.cardinality() > 0) {
          ret.saveBits(key, bits)
        }
      }
    }
    ret
  }

  def allSetBits(): Array[Long] = {
    val allSetBitsArray = new Array[Long](mSets.values.map(_.cardinality()).sum)
    var currentIdx = 0
    for ((setIndex, bitSet) <- mSets) {
      val base: Long = setIndex << VALUE_BITS
      var nextCheckPosition = 0
      while (nextCheckPosition >= 0) {
        val newSetIndex = bitSet.nextSetBit(nextCheckPosition)
        if (newSetIndex >= 0) {
          nextCheckPosition = newSetIndex + 1
          val s = base + newSetIndex
          allSetBitsArray(currentIdx) = s
          currentIdx += 1
        } else {
          nextCheckPosition = newSetIndex
        }
      }
    }
    allSetBitsArray
  }
}

private[cpslab] object LongBitSet {

  // Length of the bits for each bitset
  val VALUE_BITS = 10
  // Mask for extracting values
  val VALUE_MASK = (1 << VALUE_BITS) - 1
}
