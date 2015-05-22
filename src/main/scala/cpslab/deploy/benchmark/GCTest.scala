package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.util.Random

import com.typesafe.config.ConfigFactory
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

object GCTest {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("usage: program conf_path")
      sys.exit(1)
    }
    val conf = ConfigFactory.parseFile(new File(args(1)))
    val vectorCount = conf.getInt("vectorCount")
    val vectorDim = conf.getInt("dim")
    val zeroProbability = conf.getDouble("probability")
    val lsh = new LSH(conf)
    val threadCount = conf.getInt("parallelism")
    val startTime = System.nanoTime()
    val lshStructure = Array.fill[ConcurrentHashMap[Int, ListBuffer[SparseVector]]](
      lsh.tableIndexGenerators.length)(new ConcurrentHashMap[Int, ListBuffer[SparseVector]](16, 0.75f, 196))
    for (i <- 0 until threadCount) {
      new Thread(new Runnable {
        override def run(): Unit = {
          for (i <- 0 until vectorCount) {
            val values = Array.fill[Double](vectorDim)({
              val p = Random.nextDouble()
              if (p <= zeroProbability) 0.0 else 1.0
            })
            val t = values.zipWithIndex.filter(_._1 > 0)
            val indices = t.map(_._2)
            val nonZeroValues = t.map(_._1)
            val newVector = new SparseVector(i * vectorCount + i, vectorDim, indices, nonZeroValues)
            val bucketIndices = lsh.calculateIndex(newVector)
            for (i <- 0 until bucketIndices.length) {
              lshStructure(i).getOrElseUpdate(bucketIndices(i), new ListBuffer[SparseVector]) +=
                newVector
            }
          }
          println(System.nanoTime() - startTime)
        }
      }).start()
    }
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(1000)
        }
      }
    })
  }

}
