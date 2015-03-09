package cpslab.lsh

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import cpslab.lsh.vector.SparseVector

/**
 * Implementation class of LSH 
 * This class wraps one or more LSHTableHashChains
 * By passing different lshFamilyName, we can implement different lsh schema
 */
private[cpslab] class LSH(conf: Config) extends Serializable {

  private val lshFamilyName: String = conf.getString("cpslab.lsh.name")
  private val tableIndexGenerators: List[LSHTableHashChain[_]] = initHashChains()
  
  private def initHashChains[T <: LSHFunctionParameterSet](): List[LSHTableHashChain[_]] = {
    val familySize = conf.getInt("cpslab.lsh.familySize")
    val vectorDim = conf.getInt("cpslab.lsh.vectorDim")
    val chainLength = conf.getInt("cpslab.lsh.chainLength")
    val initializedChains = lshFamilyName match {
      case "pStable" =>
        val mu = conf.getDouble("cpslab.lsh.family.pstable.mu")
        val sigma = conf.getDouble("cpslab.lsh.family.pstable.sigma")
        val w = conf.getInt("cpslab.lsh.family.pstable.w")
        
        val family = Some(new PStableHashFamily(familySize = familySize, vectorDim = vectorDim,
          chainLength = chainLength, pStableMu = mu, pStableSigma = sigma, w = w))
        pickUpHashChains(family)
      case x => None
    }
    if (initializedChains.isDefined) {
      initializedChains.get
    } else {
      List()
    }
  }
  
  private def pickUpHashChains[T <: LSHFunctionParameterSet](lshFamily: Option[LSHHashFamily[T]]):
  Option[List[LSHTableHashChain[T]]] = {
    require(lshFamily.isDefined, s"$lshFamilyName is not a valid family name")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val generateMethodOfHashFamily = conf.getString("cpslab.lsh.generateMethod")
    lshFamily.map(lshHashFamily => {
      if (generateMethodOfHashFamily == "default") {
        lshHashFamily.pick(tableNum)
      } else if (generateMethodOfHashFamily == "fromfile"){
        lshHashFamily.generateTableChainFromFile(conf.getString("cpslab.lsh.familyFilePath"), 
          tableNum) 
      } else {
        null
      }
    })
  }
  
  
  /**
   * calculate the index of the vector in tables
   * @param vector the vector to be indexed
   * @param validTableIDs the tables where this vector to be put in. If not passing this parameter
   *                      in, the index of the vector in all tables will be calculated
   * @return the index of the vector in tables, the order corresponds to the validTableIDs parameter
   */
  def calculateIndex(vector: SparseVector, validTableIDs: Seq[Int] = 
    0 until tableIndexGenerators.size): Array[Array[Byte]] = {
    (for (i <- 0 until tableIndexGenerators.size if validTableIDs.contains(i))
      yield tableIndexGenerators(i).compute(vector)).toArray
  }
}

private[lsh] object LSH {

  private[lsh] def generateParameterSetString(config: Config): String = {
    val sb = new StringBuffer()
    val lsh = new LSH(config)
    lsh.tableIndexGenerators.foreach(hashChain =>
      hashChain.chainIndexCalculator.foreach(hashParameterSet =>
        sb.append(hashParameterSet.toString))
    )
    sb.toString
  }
  
  // output the generated parameters
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("usage: program configFilePath")
      sys.exit(1)
    }
    val config = ConfigFactory.parseFile(new File(args(0)))
    println(generateParameterSetString(config))
  }
}
