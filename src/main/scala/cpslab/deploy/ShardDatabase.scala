package cpslab.deploy

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.mapdb.DBMaker.Maker
import org.mapdb.{DBMaker, HTreeMap, Serializer}

private[deploy] object ShardDatabase {

  var actors: Seq[ActorRef] = null

  @volatile var startTime = 0L
  @volatile var endTime = 0L

  class InitializeWorker(parallelism: Int, lsh: LSH) extends Actor {

    context.setReceiveTimeout(20000 milliseconds)

    override def postStop(): Unit = {
      val totalTime = endTime - startTime
      println("table building cost " + totalTime + " milliseconds")
    }

    override def receive: Receive = {
      case sv: SparseVector =>
        if (startTime == 0) {
          startTime = System.currentTimeMillis()
        }
        val bucketIndices = lsh.calculateIndex(sv)
        for (i <- 0 until bucketIndices.length) {
          val bucketIndex = bucketIndices(i)
          vectorDatabase(i).putIfAbsent(bucketIndex, new ConcurrentLinkedQueue[Int]())
          vectorDatabase(i).get(bucketIndex).add(sv.vectorId)
        }
        val currentTime = System.currentTimeMillis()
        if (currentTime > endTime) {
          endTime = currentTime
        }
      case ReceiveTimeout =>
        context.stop(self)
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
    def initializeVectorDatabase(): HTreeMap[Int, ConcurrentLinkedQueue[Int]] =  {
      val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
        counterEnable().
        keySerializer(Serializer.INTEGER)
      hashMapMaker.make[Int, ConcurrentLinkedQueue[Int]]()
    }
    def initializeIdToVectorMap(): HTreeMap[Int, SparseVector] = {
      val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
        counterEnable().
        keySerializer(Serializer.INTEGER)
      hashMapMaker.make[Int, SparseVector]()
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
      parallelism: Int): Unit = {
    actors = {
      for (i <- 0 until parallelism)
        yield actorSystem.actorOf(Props(new InitializeWorker(parallelism, lsh)))
    }
    val allFiles = Utils.buildFileListUnderDirectory(filePath)
    for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
      val (id, size, indices, values) = Vectors.fromString1(line)
      val vector = new SparseVector(id, size, indices, values)
      vectorIdToVector.put(vector.vectorId, vector)
      actors(Random.nextInt(parallelism)) ! vector
    }
  }

  private[deploy] var vectorDatabase: Array[HTreeMap[Int, ConcurrentLinkedQueue[Int]]] = null
  private[deploy] var vectorIdToVector: HTreeMap[Int, SparseVector] = null

  
}
