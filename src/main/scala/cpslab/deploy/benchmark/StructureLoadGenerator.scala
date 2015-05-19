package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ListBuffer
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Random

import com.typesafe.config.{ConfigFactory, Config}
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

object StructureLoadGenerator {

  var startTime = 0L

  val vectors = new ListBuffer[SparseVector]

  def runWriteLoadOnLSH(lsh: LSH): Unit = {
    val lshIndex = new LSHIndex(lsh)
    for (vector <- vectors) {
      future {
        lshIndex.insert(vector)
      }.onSuccess {
        case x =>
          if (lshIndex.totalCount.get() >= vectors.length) {
            println("total time cost:" + (System.nanoTime() - startTime))
            runReadLoadOnLSH(lsh)
          }
      }
    }
  }

  def runReadLoadOnLSH(lsh: LSH): Unit = {
    startTime = System.nanoTime()
    val finishedCount = new AtomicInteger(0)
    val lshIndex = new LSHIndex(lsh)
    for (vector <- vectors) {
      future {
        lshIndex.query(vector)
      }.onSuccess{
        case x =>
          if (finishedCount.incrementAndGet() >= vectors.length) {
            println("total time cost:" + (System.nanoTime() - startTime))
            sys.exit(0)
          }
      }
    }
  }

  def runWriteLoadOnIndex(dim: Int): Unit = {
    startTime = System.nanoTime()
    val invertedIndex = new InvertedIndex(dim)
    for (vector <- vectors) {
      future {
        invertedIndex.insert(vector)
      }.onSuccess {
        case x =>
          if (invertedIndex.totalCount.get() >= vectors.length) {
            println("total time cost:" + (System.nanoTime() - startTime))
            runReadLoadOnIndex(dim)
          }
      }
    }
  }

  def runReadLoadOnIndex(dim: Int): Unit = {
    startTime = System.nanoTime()
    val finishedCount = new AtomicInteger(0)
    val invertedIndex = new InvertedIndex(dim)
    for (vector <- vectors) {
      val f = future {
        invertedIndex.query(vector)
      }.onSuccess {
        case x =>
          if (finishedCount.incrementAndGet() >= vectors.length) {
            println("total time cost:" + (System.nanoTime() - startTime))
            sys.exit(0)
          }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: program structure conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(1)))
    val lsh = new LSH(conf)
    val vectorCount = conf.getInt("vectorCount")
    val vectorDim = conf.getInt("dim")
    val zeroProbability = conf.getDouble("probability")
    for (i <- 0 until vectorCount) {
      val values = Array.fill[Double](vectorDim)({
        val p = Random.nextDouble()
        if (p <= zeroProbability) 0.0 else  1.0
      })
      val indices = values.zipWithIndex.filter(_._1 > 0).map(_._2)
      val nonZeroValues = values.filter(_ > 0)
      val newVector = new SparseVector(i, indices.length, indices, nonZeroValues)
      vectors += newVector
    }
    println("Finished generating vectors")
    if (args(0) == "index") {
      runWriteLoadOnIndex(vectorDim)
    }
    if (args(0) == "lsh") {
      runWriteLoadOnLSH(lsh)
    }
    Thread.sleep(1000000)
  }
}
