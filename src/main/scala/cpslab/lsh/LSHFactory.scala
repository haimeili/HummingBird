package cpslab.lsh

import cpslab.lsh.LSHType._
import cpslab.lsh.base.LSHBaseFactory
import cpslab.util.Configuration

trait LSHFactory {
  def newInstance(): LSH
}

object LSHFactory {
  private[lsh] var familySize = 0
  private[lsh] var vectorDimension = 0
  private[lsh] var chainLength = 0
  private[lsh] var chainNum = 0

  def apply(lshType: LSHType, config: Configuration): LSHFactory = {
    LSHFactory.familySize = config.getInt("cpslab.lshquery.lsh.familySize")
    LSHFactory.vectorDimension = config.getInt("cpslab.lshquery.lsh.vectorSize")
    LSHFactory.chainLength = config.getInt("cpslab.lshquery.lsh.chainLength")
    LSHFactory.chainNum = config.getInt("cpslab.lshquery.lsh.chainNum")
    if (familySize <= chainLength) {
      throw new IllegalArgumentException(("familySize must be larger than chainLength, " +
        "the current value %d, %d").format(familySize, chainLength))
    }
    if (lshType == BASE) {
      return new LSHBaseFactory(config)
    }
    null
  }
}
