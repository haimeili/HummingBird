package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.{Executors, ConcurrentHashMap}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.util.Random

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorSystem}
import com.typesafe.config.ConfigFactory
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector

object ConcurrencyTest {

  var threadCount = 0
  var batchSize = 0
  val vectors = new ListBuffer[SparseVector]
  val totalCount = new AtomicInteger(0)

  implicit lazy val executorService : ExecutionContext = {
    val executorService = Executors.newFixedThreadPool(threadCount)
    ExecutionContext.fromExecutorService(executorService)
  }

  def runWithGlobalLock(lsh: LSH)(implicit executionContext: ExecutionContext): Unit = {
    val lshStructure = Array.fill[mutable.HashMap[Int, ListBuffer[SparseVector]]](
      lsh.tableIndexGenerators.length)(new mutable.HashMap[Int, ListBuffer[SparseVector]])
    for (i <- 0 until threadCount) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          var writeCount = 0
          val startTime = System.nanoTime()
          var vectorIdx = 0
          while (true) {
            val vector = vectors(vectorIdx)
            val indices = lsh.calculateIndex(vector)
            for (i <- 0 until indices.length) {
              lshStructure(i).synchronized {
                lshStructure(i).get(indices(i))
                lshStructure(i).getOrElseUpdate(indices(i), new ListBuffer[SparseVector]) += vector
                writeCount += 1
              }
            }
            if (vectorIdx % batchSize == 0) {
              Thread.sleep(Random.nextInt(100))
            }
            if (vectorIdx == vectors.length - 1) {
              vectorIdx = 0
            } else {
              vectorIdx += 1
            }
            if (System.nanoTime() - startTime > 2000000000) {
              println("counting in " + Thread.currentThread().getName + ": " + writeCount)
              return
            }
          }
        }
      })
    }
  }

  def runWithLockStripping(lsh: LSH)(implicit executionContext: ExecutionContext): Unit = {
    val lshStructure = Array.fill[LockStrippingHashMap](
      lsh.tableIndexGenerators.length)(new LockStrippingHashMap)
    for (i <- 0 until threadCount) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          var writeCount = 0
          val startTime = System.nanoTime()
          var vectorIdx = 0
          while (true) {
            val vector = vectors(vectorIdx)
            val indices = lsh.calculateIndex(vector)
            for (i <- 0 until indices.length) {
              lshStructure(i).get(indices(i))
              lshStructure(i).put(indices(i), vector)
              writeCount += 1
            }
            if (vectorIdx % batchSize == 0) {
              Thread.sleep(Random.nextInt(100))
            }
            if (vectorIdx == vectors.length - 1) {
              vectorIdx = 0
            } else {
              vectorIdx += 1
            }
            if (System.nanoTime() - startTime > 2000000000) {
              println("counting in " + Thread.currentThread().getName + ": " + writeCount)
              return
            }
          }
        }
      })
    }
  }

  def runWithVolatile(lsh: LSH)(implicit executionContext: ExecutionContext): Unit = {
    val lshStructure = Array.fill[VolatileHashMap](
      lsh.tableIndexGenerators.length)(new VolatileHashMap)
    for (i <- 0 until threadCount) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          var writeCount = 0
          val startTime = System.nanoTime()
          var vectorIdx = 0
          while (true) {
            val vector = vectors(vectorIdx)
            val indices = lsh.calculateIndex(vector)
            for (i <- 0 until indices.length) {
              lshStructure(i).get(indices(i))
              lshStructure(i).put(indices(i), vector)
              writeCount += 1
            }
            if (vectorIdx % batchSize == 0) {
              Thread.sleep(Random.nextInt(100))
            }
            if (vectorIdx == vectors.length - 1) {
              vectorIdx = 0
            } else {
              vectorIdx += 1
            }
            if (System.nanoTime() - startTime > 2000000000) {
              println("counting in " + Thread.currentThread().getName + ": " + writeCount)
              return
            }
          }
        }
      })
    }
  }

  def runWithVolatileStrip(lsh: LSH)(implicit executionContext: ExecutionContext): Unit = {
    val lshStructure = Array.fill[ConcurrentHashMap[Int, ListBuffer[SparseVector]]](
      lsh.tableIndexGenerators.length)(new ConcurrentHashMap[Int, ListBuffer[SparseVector]](16, 0.75f, 196))
    for (i <- 0 until threadCount) {
      executionContext.execute(new Runnable {
        override def run(): Unit = {
          var writeCount = 0
          val startTime = System.nanoTime()
          var vectorIdx = 0
          while (true) {
            val vector = vectors(vectorIdx)
            val indices = lsh.calculateIndex(vector)
            for (i <- 0 until indices.length) {
              lshStructure(i).get(indices(i))
              lshStructure(i).putIfAbsent(indices(i), new ListBuffer[SparseVector])
              lshStructure(i).get(indices(i)) += vector
              writeCount += 1
            }
            if (vectorIdx % batchSize == 0) {
              Thread.sleep(Random.nextInt(100))
            }
            if (vectorIdx == vectors.length - 1) {
              vectorIdx = 0
            } else {
              vectorIdx += 1
            }
            if (System.nanoTime() - startTime > 2000000000) {
              println("counting in " + Thread.currentThread().getName + ": " + writeCount)
              return
            }
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
    threadCount = conf.getInt("parallelism")
    batchSize = conf.getInt("batchSize")
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
    args(0) match {
      case "global" =>
        runWithGlobalLock(lsh)
      case "lockstripping" =>
        runWithLockStripping(lsh)
      case "volatile" =>
        runWithVolatile(lsh)
      case "volatileStripping" =>
        runWithVolatileStrip(lsh)
    }
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(1000)
        }
      }
    }).start()
  }
}
