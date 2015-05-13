package cpslab.deploy

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, ConcurrentMap}

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.Config
import cpslab.deploy.benchmark.DataSetLoader
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector
import org.mapdb.DBMaker.Maker
import org.mapdb.{DBMaker, Serializer}

private[deploy] object ShardDatabase extends DataSetLoader {

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
      case Report =>
    }
  }

  class InitializeWorker(parallelism: Int, lsh: LSH) extends Actor {

    context.setReceiveTimeout(30000 milliseconds)

    private val monitor = context.actorSelection("/user/monitor")
    private var hasSentReport = false

    override def receive: Receive = {
      case sv: SparseVector =>
        if (hasSentReport) {
          monitor ! Report
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
          l.add(sv.vectorId)
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
    val memModel = conf.getString("cpslab.vectorDatabase.memoryModel")
    val dbMem = memModel match {
      case "onheap" =>
        DBMaker.memoryDB()
      case "offheap" =>
        DBMaker.memoryUnsafeDB()
    }
    var dbMaker = dbMem.transactionDisable()
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
      parallelism: Int,
      tableNum: Int,
      replica: Int,
      offset: Int,
      cap: Int): Unit = {
    actors = {
      for (i <- 0 until parallelism)
        yield actorSystem.actorOf(Props(new InitializeWorker(parallelism, lsh)))
    }
    initVectorDatabaseFromFS(filePath, replica, offset, cap)
    // start monitor actor
    actorSystem.actorOf(Props(new MonitorActor), name = "monitor")
    //start writing rate monitor thread
    new Thread(
      new Runnable {
        override def run(): Unit = {
          var lastAmount = 0L
          var lastTime = 0L
          while (true) {
            var totalCnt = 0
            for (i <- 0 until vectorDatabase.length) {
              val entrySetItr = vectorDatabase(i).entrySet().iterator()
              while (entrySetItr.hasNext) {
                val a = entrySetItr.next()
                totalCnt += a.getValue.size()
              }
            }
            val currentTime = System.nanoTime()
            println(s"Writing Rate ${(totalCnt - lastAmount) * 1.0 /
              ((currentTime - lastTime) / 1000000000)}")
            lastAmount = totalCnt
            lastTime = currentTime
            Thread.sleep(1000)
          }
        }
      }
    ).start()
    val itr = vectorIdToVector.values().iterator()
    while (itr.hasNext) {
      val vector = itr.next()
      actors(vector.vectorId % parallelism) ! vector
    }
  }

  private[deploy] var vectorDatabase: Array[ConcurrentMap[Int, ConcurrentLinkedQueue[Int]]] = null
  private[deploy] var vectorIdToVector: ConcurrentMap[Int, SparseVector] = null

  
}
