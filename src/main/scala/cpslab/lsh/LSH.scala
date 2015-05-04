package cpslab.lsh

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import com.typesafe.config.{Config, ConfigFactory}
import cpslab.lsh.vector.SparseVector

/**
 * Implementation class of LSH 
 * This class wraps one or more LSHTableHashChains
 * By passing different lshFamilyName, we can implement different lsh schema
 */
private[cpslab] class LSH(conf: Config) extends Serializable {

  private val lshFamilyName: String = conf.getString("cpslab.lsh.name")
  //TODO: to implement two-level partition mechanism in PLSH, we have to expose this variable to
  // external side; we can actually fix it with Dependency Injection, etc.?
  private[cpslab] val tableIndexGenerators: List[LSHTableHashChain[_]] = initHashChains()
  
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
      case "angle" =>
        val family = Some(new AngleHashFamily(familySize = familySize, vectorDim = vectorDim,
          chainLength = chainLength))
        pickUpHashChains(family)
      case "precalculated" =>
        val family = Some(new PrecalculatedHashFamily(
          familySize = familySize, vectorDim = vectorDim, chainLength = chainLength))
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
   * calculate the index of the vector in tables, the index in each table is represented as a 
   * byte array
   * @param vector the vector to be indexed
   * @return the index of the vector in tables, the order corresponds to the validTableIDs parameter
   */
  def calculateIndex(vector: SparseVector): Array[Int] = {
    (for (i <- 0 until tableIndexGenerators.size)
      yield tableIndexGenerators(i).compute(vector)).toArray
  }
}

private[lsh] object LSH {

  private[lsh] def generateParameterSetString(config: Config): String = {
    val sb = new StringBuffer()
    val lsh = new LSH(config)
    lsh.tableIndexGenerators.foreach(hashChain =>
      hashChain.chainedHashFunctions.foreach(hashParameterSet =>
        sb.append(hashParameterSet.toString + "\n"))
    )
    sb.toString
  }
  
  // output the generated parameters
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("usage: program configFilePath")
      sys.exit(1)
    }
    val config = ConfigFactory.parseString(
      """
        |cpslab.lsh.generateMethod=default
      """.stripMargin).withFallback(ConfigFactory.parseFile(new File(args(0))))
    val str = generateParameterSetString(config)
    Files.write(Paths.get("file.txt"), str.getBytes(StandardCharsets.UTF_8))
  }
}
