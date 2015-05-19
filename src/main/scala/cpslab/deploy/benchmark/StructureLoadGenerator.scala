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
    startTime = System.nanoTime()
    val lshIndex = new LSHIndex(lsh)
    for (vector <- vectors) {
      future {
        lshIndex.insert(vector)
      }.onComplete {
        case x =>

      }
    }
  }

  def runReadLoadOnLSH(lshIndex: LSHIndex): Unit = {
    startTime = System.nanoTime()
    for (vector <- vectors.take(1000)) {
      future {
        lshIndex.query(vector)
      }.onComplete {
        case x =>

      }
    }
  }

  def runWriteLoadOnIndex(dim: Int): Unit = {
    startTime = System.nanoTime()
    val invertedIndex = new InvertedIndex(dim)
    for (vector <- vectors) {
      val f = future {
        invertedIndex.insert(vector)
      }.onComplete {
        case x =>

      }
    }
  }

  def runReadLoadOnIndex(invertedIndex: InvertedIndex, dim: Int): Unit = {
    startTime = System.nanoTime()
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors.take(1000)) {
      val f = future {
        invertedIndex.query(vector)
      }.onComplete {
        case x =>
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
      val t = values.zipWithIndex.filter(_._1 > 0)
      val indices = t.map(_._2)
      val nonZeroValues = t.map(_._1)
      val newVector = new SparseVector(i, vectorDim, indices, nonZeroValues)
      vectors += newVector
    }
    println("Finished generating vectors")
    if (args(0) == "index") {
      runWriteLoadOnIndex(vectorDim)
    }
    if (args(0) == "lsh") {
      runWriteLoadOnLSH(lsh)
    }
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          println("elapse time:" + (System.nanoTime() - startTime))
          Thread.sleep(1000)
        }
      }
    }).start()
    Thread.sleep(10000000)
  }
}
