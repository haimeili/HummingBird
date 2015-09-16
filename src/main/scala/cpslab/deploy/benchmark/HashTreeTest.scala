package cpslab.deploy.benchmark

import java.io.File
import java.util.concurrent.Executors

import scala.util.Random

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
    for (t <- 0 until threadNumber) {
      threadPool.execute(new Runnable {
        override def run(): Unit = {
          System.out.println("startTime: " + System.nanoTime())
          for (i <- 0 until requestNumberPerThread) {
            val interestVectorId = Random.nextInt(conf.getInt("cpslab.lsh.benchmark.cap"))
            for (tableId <- 0 until conf.getInt("cpslab.lsh.tableNum")) {
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
    init(conf)
    testThreadScalability(conf, 500, 20)
  }
}
