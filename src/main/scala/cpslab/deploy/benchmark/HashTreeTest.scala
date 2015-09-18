package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}
import java.util.concurrent.{Executors, ForkJoinPool}

import scala.io.Source
import scala.util.Random

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{Utils, LSHServer, ShardDatabase}
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}

object HashTreeTest {

  def testWriteThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {
    ShardDatabase.initializeMapDBHashMap(conf)

    val filePath = conf.getString("cpslab.lsh.inputFilePath")
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    for (i <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        val base = i
        var totalTime = 0L
        override def run(): Unit = {
          var cnt = 0
          val allFiles = Random.shuffle(Utils.buildFileListUnderDirectory(filePath))
          for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
            val (_, size, indices, values) = Vectors.fromString1(line)
            val vector = new SparseVector(cnt + cap * base, size, indices, values)
            if (cnt > cap) {
              return
            }
            val s = System.nanoTime()
            vectorIdToVector.put(vector.vectorId, vector)
            for (i <- 0 until tableNum) {
              vectorDatabase(i).put(vector.vectorId, true)
            }
            val e = System.nanoTime()
            totalTime += e - s
            cnt += 1
          }
          println(cap / (totalTime / 1000000000))
        }
      })
    }
  }

  def testReadThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {

    ShardDatabase.initializeMapDBHashMap(conf)
    //init database by filling vectors
    ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.replica"),
      conf.getInt("cpslab.lsh.benchmark.offset"),
      conf.getInt("cpslab.lsh.benchmark.cap"))

    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val cap = conf.getInt("cpslab.lsh.benchmark.cap")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          System.out.println("startTime: " + System.nanoTime())
          for (i <- 0 until requestNumberPerThread) {
            val interestVectorId = Random.nextInt(cap)
            for (tableId <- 0 until tableNum) {
              ShardDatabase.vectorDatabase(tableId).getSimilar(interestVectorId)
            }
          }
          System.out.println("endTime: " + System.nanoTime())
        }
      })
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    LSHServer.lshEngine = new LSH(conf)
    val requestPerThread = conf.getInt("cpslab.lsh.benchmark.requestNumberPerThread")
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")
    //testReadThreadScalability(conf, requestNumberPerThread = requestPerThread,
    //  threadNumber = threadNumber)
    testWriteThreadScalability(conf, requestPerThread, threadNumber)
  }
}
