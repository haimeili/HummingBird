package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray}
import java.util.concurrent.{Executors, ForkJoinPool}

import scala.util.Random

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import cpslab.deploy.{LSHServer, ShardDatabase}
import cpslab.lsh.LSH

object HashTreeTest {

  def init(conf: Config): Unit = {
    ShardDatabase.initializeMapDBHashMap(conf)
    //init database by filling vectors
    ShardDatabase.initVectorDatabaseFromFS(
      conf.getString("cpslab.lsh.inputFilePath"),
      conf.getInt("cpslab.lsh.benchmark.replica"),
      conf.getInt("cpslab.lsh.benchmark.offset"),
      conf.getInt("cpslab.lsh.benchmark.cap"))
  }

  def testThreadScalability(
    conf: Config,
    requestNumberPerThread: Int,
    threadNumber: Int): Unit = {
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    val startTime = System.nanoTime()
    val count = new AtomicLong()
    for (t <- 0 until threadNumber * requestNumberPerThread) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          val cap = conf.getInt("cpslab.lsh.benchmark.cap")
          val tableNum = conf.getInt("cpslab.lsh.tableNum")
          val interestVectorId = Random.nextInt(cap)
          for (tableId <- 0 until tableNum) {
            ShardDatabase.vectorDatabase(tableId).getSimilar(interestVectorId)
          }
          if (count.incrementAndGet() == threadNumber * requestNumberPerThread) {
            println(threadNumber * requestNumberPerThread /
              ((System.nanoTime() - startTime) / 1000000000))
          }
        }
      })
    }

    /*
    val threadPool = Executors.newFixedThreadPool(threadNumber)
    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          val cap = conf.getInt("cpslab.lsh.benchmark.cap")
          val tableNum = conf.getInt("cpslab.lsh.tableNum")
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
    }*/
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.parseFile(new File(args(0)))
    LSHServer.lshEngine = new LSH(conf)
    init(conf)
    val requestPerThread = conf.getInt("cpslab.lsh.benchmark.requestNumberPerThread")
    val threadNumber = conf.getInt("cpslab.lsh.benchmark.threadNumber")
    testThreadScalability(conf, requestNumberPerThread = requestPerThread,
      threadNumber = threadNumber)
  }
}
