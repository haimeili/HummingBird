package cpslab.deploy

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ConcurrentMap}

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.mapdb.DBMaker.Maker
import org.mapdb.{DBMaker, Serializer}

private[deploy] object ShardDatabase {

  var actors: Seq[ActorRef] = null

  class InitializeWorker(parallelism: Int, lsh: LSH) extends Actor {

    override def receive: Receive = {
      case sv: SparseVector =>
        val bucketIndices = lsh.calculateIndex(sv)
        for (i <- 0 until bucketIndices.length) {
          val bucketIndex = bucketIndices(i)
          vectorDatabase(i).putIfAbsent(bucketIndex, new ConcurrentLinkedQueue[Int]())
          vectorDatabase(i).get(bucketIndex).add(sv.vectorId)
        }
    }
  }

  private def initDBMaker(conf: Config): Maker = {
    var dbMaker = DBMaker.
      memoryDirectDB().
      transactionDisable()
    val asyncDelay = conf.getInt("cpslab.vectorDatabase.asyncDelay")
    if (asyncDelay > 0) {
      val asyncQueueSize = conf.getInt("cpslab.vectorDatabase.asyncQueueSize")
      dbMaker = dbMaker.asyncWriteEnable().asyncWriteFlushDelay(asyncDelay).
        asyncWriteQueueSize(asyncQueueSize)
    }
    dbMaker
  }

  def initializeMapDBHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    def initializeVectorDatabase(): ConcurrentMap[Int, ConcurrentLinkedQueue[Int]] =
      concurrentCollectionType match {
        case "MapDBHashMap" =>
          val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
            counterEnable().
            keySerializer(Serializer.INTEGER)
          hashMapMaker.make[Int, ConcurrentLinkedQueue[Int]]()
        case "ConcurrentHashMap" =>
          new ConcurrentHashMap[Int, ConcurrentLinkedQueue[Int]]()
      }
    def initializeIdToVectorMap(): ConcurrentMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "MapDBHashMap" =>
          val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
            counterEnable().
            keySerializer(Serializer.INTEGER)
          hashMapMaker.make[Int, SparseVector]()
        case "ConcurrentHashMap" =>
          new ConcurrentHashMap[Int, SparseVector]()
      }
    vectorDatabase = Array.fill(tableNum)(initializeVectorDatabase())
    vectorIdToVector = initializeIdToVectorMap()
  }



  /**
   * initialize the database by reading raw vector data from file system
   * @param filePath the root path of the data directory
   * @param parallelism the number of actors writing data
   */
  def initVectorDatabaseFromFS(
      lsh: LSH,
      actorSystem: ActorSystem,
      filePath: String,
      tableNum: Int,
      totalVectorNum: Int,
      parallelism: Int): Unit = {
    actors = {
      for (i <- 0 until parallelism)
        yield actorSystem.actorOf(Props(new InitializeWorker(parallelism, lsh)))
    }
    if (filePath != "") {
      val allFiles = Utils.buildFileListUnderDirectory(filePath)
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        val (id, size, indices, values) = Vectors.fromString1(line)
        val vector = new SparseVector(id, size, indices, values)
        vectorIdToVector.put(vector.vectorId, vector)
        actors(Random.nextInt(parallelism)) ! vector
      }
    }
    new Thread() {
      override def run(): Unit = {
        var stopSign = false
        val startTime = System.currentTimeMillis()
        while (!stopSign) {
          Thread.sleep(100)
          for (i <- 0 until tableNum) {
            stopSign = vectorDatabase(i).size() >= totalVectorNum
          }
        }
        println(s"total Time: ${System.currentTimeMillis() - startTime}")
      }
    }
  }

  private[deploy] var vectorDatabase: Array[ConcurrentMap[Int, ConcurrentLinkedQueue[Int]]] = null
  private[deploy] var vectorIdToVector: ConcurrentMap[Int, SparseVector] = null

  
}
