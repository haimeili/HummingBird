package cpslab.deploy

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ConcurrentMap}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.mapdb.DBMaker.Maker
import org.mapdb.{DBMaker, Serializer}

private[deploy] object ShardDatabase {

  var actors: Seq[ActorRef] = null
  @volatile var startTime = -1L
  @volatile var endTime = -1L

  case object Report

  class MonitorActor extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    override def receive: Receive = {
      case ReceiveTimeout =>
        println("Finished building table: " + (endTime - startTime) + " milliseconds")
        println("Monitor Actor Stopped")
        actors.foreach(actor => actor ! PoisonPill)
      case Report =>
    }
  }

  class InitializeWorker(parallelism: Int, lsh: LSH) extends Actor {

    context.setReceiveTimeout(60000 milliseconds)

    private val monitor = context.actorSelection("/user/monitor")
    private var hasSentReport = false

    override def receive: Receive = {
      case sv: SparseVector =>
        if (hasSentReport) {
          hasSentReport = false
        }
        if (startTime == -1L) {
          startTime = System.currentTimeMillis()
        }
        val bucketIndices = lsh.calculateIndex(sv)
        for (i <- 0 until bucketIndices.length) {
          val bucketIndex = bucketIndices(i)
          vectorDatabase(i).putIfAbsent(bucketIndex, new ConcurrentLinkedQueue[Int]())
          val l = vectorDatabase(i).get(bucketIndex)
          vectorDatabase(i).put(bucketIndex, l)
        }
        val endMoment = System.currentTimeMillis()
        if (endMoment > endTime) {
          endTime = endMoment
        }
      case ReceiveTimeout =>
        if (!hasSentReport) {
          monitor ! Report
          hasSentReport = true
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
          new ConcurrentHashMap[Int, ConcurrentLinkedQueue[Int]](16, 0.75f, 196)
      }
    def initializeIdToVectorMap(): ConcurrentMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "MapDBHashMap" =>
          val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
            counterEnable().
            keySerializer(Serializer.INTEGER)
          hashMapMaker.make[Int, SparseVector]()
        case "ConcurrentHashMap" =>
          new ConcurrentHashMap[Int, SparseVector](16, 0.75f, 196)
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
    if (filePath != "") {
      val allFiles = Utils.buildFileListUnderDirectory(filePath)
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        val (id, size, indices, values) = Vectors.fromString1(line)
        val vector = new SparseVector(id, size, indices, values)
        vectorIdToVector.put(vector.vectorId, vector)
      }
    }
    println("Finished Loading Data")
    //start monitor actor
    actorSystem.actorOf(Props(new MonitorActor), name = "monitor")
    val itr = vectorIdToVector.values().iterator()
    while (itr.hasNext) {
      val vector = itr.next()
      actors(vector.vectorId % parallelism) ! vector
    }
  }

  private[deploy] var vectorDatabase: Array[ConcurrentMap[Int, ConcurrentLinkedQueue[Int]]] = null
  private[deploy] var vectorIdToVector: ConcurrentMap[Int, SparseVector] = null

  
}
