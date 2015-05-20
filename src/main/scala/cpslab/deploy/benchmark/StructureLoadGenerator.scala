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

  var queryNumber = 0
  val vectors = new ListBuffer[SparseVector]
  val performanceMeasurement = new ListBuffer[Long]

  def runWriteLoadOnLSH(lsh: LSH)(implicit executionContext: ExecutionContext): Unit = {
    val lshIndex = new LSHIndex(lsh)
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          val startTime = System.nanoTime()
          lshIndex.insert(vector)
          val performance = System.nanoTime() - startTime
          performanceMeasurement += performance
          if (finishedCount.incrementAndGet() == vectors.length) {
            //get average time cost
            val avr = performanceMeasurement.sum * 1.0 / performanceMeasurement.size
            println("write time cost with lsh " + avr)
            performanceMeasurement.clear()
            runReadLoadOnLSH(lshIndex)
          }
        }
      })
    }
  }

  def runReadLoadOnLSH(lshIndex: LSHIndex)(implicit executionContext: ExecutionContext): Unit = {
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          val startTime = System.nanoTime()
          lshIndex.query(vector)
          val performance = System.nanoTime() - startTime
          performanceMeasurement += performance
          if (finishedCount.incrementAndGet() == queryNumber) {
            val avr = performanceMeasurement.sum * 1.0 / performanceMeasurement.size
            println("read time cost with lsh " + avr)
            sys.exit(0)
          }
        }
      }
      )
    }
  }

  def runWriteLoadOnIndex(dim: Int)(implicit executionContext: ExecutionContext): Unit = {
    val invertedIndex = new InvertedIndex(dim)
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          val startTime = System.nanoTime()
          invertedIndex.insert(vector)
          val performance = System.nanoTime() - startTime
          performanceMeasurement += performance
          if (finishedCount.incrementAndGet() == vectors.length) {
            val avr = performanceMeasurement.sum * 1.0 / performanceMeasurement.size
            println("write time cost with inverted index " + avr)
            performanceMeasurement.clear()
            runReadLoadOnIndex(invertedIndex, dim)
          }
        }
      })
    }
  }

  def runReadLoadOnIndex(invertedIndex: InvertedIndex, dim: Int)
                        (implicit executionContext: ExecutionContext): Unit = {
    val finishedCount = new AtomicInteger(0)
    for (vector <- vectors.take(1000)) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          val startTime = System.nanoTime()
          invertedIndex.query(vector)
          val performance = System.nanoTime() - startTime
          performanceMeasurement += performance
          if (finishedCount.incrementAndGet() == queryNumber) {
            val avr = performanceMeasurement.sum * 1.0 / performanceMeasurement.size
            println("read time cost with inverted index " + avr)
            performanceMeasurement.clear()
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
    queryNumber = conf.getInt("queryNumber")
    if (args(0) == "index") {
      runWriteLoadOnIndex(vectorDim)
    }
    if (args(0) == "lsh") {
      runWriteLoadOnLSH(lsh)
    }
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(1000)
        }
      }
    }).start()
    Thread.sleep(1000)
  }
}
