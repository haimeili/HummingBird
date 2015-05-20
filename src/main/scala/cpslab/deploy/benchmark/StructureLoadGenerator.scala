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

  def runWriteLoadOnLSH(lsh: LSH)(implicit executionContext: ExecutionContext): Unit = {
    startTime = System.nanoTime()
    val lshIndex = new LSHIndex(lsh)
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          lshIndex.insert(vector)
          if (finishedCount.incrementAndGet() == vectors.length) {
            println("write time cost with lsh " + (System.nanoTime() - startTime))
            startTime = System.nanoTime()
            runReadLoadOnLSH(lshIndex)
          }
        }
      })
    }
  }

  def runReadLoadOnLSH(lshIndex: LSHIndex)(implicit executionContext: ExecutionContext): Unit = {
    startTime = System.nanoTime()
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors.take(1000)) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          lshIndex.query(vector)
          if (finishedCount.incrementAndGet() == vectors.length) {
            println("read time cost with lsh " + (System.nanoTime() - startTime))
            sys.exit(0)
          }
        }
      }
      )
    }
  }

  def runWriteLoadOnIndex(dim: Int)(implicit executionContext: ExecutionContext): Unit = {
    startTime = System.nanoTime()
    val invertedIndex = new InvertedIndex(dim)
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          invertedIndex.insert(vector)
          if (finishedCount.incrementAndGet() == vectors.length) {
            println("write time cost with inverted index " + (System.nanoTime() - startTime))
            startTime = System.nanoTime()
            runReadLoadOnIndex(invertedIndex, dim)
          }
        }
      })
    }
  }

  def runReadLoadOnIndex(invertedIndex: InvertedIndex, dim: Int)
                        (implicit executionContext: ExecutionContext): Unit = {
    startTime = System.nanoTime()
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors.take(1000)) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          invertedIndex.query(vector)
          if (finishedCount.incrementAndGet() == 1000) {
            println("read time cost with inverted index " + (System.nanoTime() - startTime))
            sys.exit(0)
          }
        }
      })
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
        if (p <= zeroProbability) 0.0 else 1.0
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
    Thread.sleep(10000000)
  }
}
