package cpslab.lsh

import cpslab.lsh.LSHType._
import cpslab.util.Configuration

class LSHFactory(config: Configuration) {
  LSHFactory.familySize = config.getInt("cpslab.lshquery.lsh.familySize")
  LSHFactory.familyNum = config.getInt("cpslab.lshquery.lsh.familyNum")
  LSHFactory.vectorDimension = config.getInt("cpslab.lshquery.lsh.vectorSize")
  LSHFactory.chainLength = config.getInt("cpslab.lshquery.lsh.chainLength")
  LSHFactory.chainNum = config.getInt("cpslab.lshquery.lsh.chainNum")


  def newInstance(config: Configuration, lshType: LSHType): LSH = {
    if (lshType == BASE) {
      new LSHBase(config)
    }
    null
  }
}

object LSHFactory {
  private[lsh] var familySize = 0
  private[lsh] var familyNum = 0
  private[lsh] var vectorDimension = 0
  private[lsh] var chainLength = 0
  private[lsh] var chainNum = 0
}
