package cpslab.deploy

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import akka.actor._
import com.typesafe.config.Config
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SparseVector, Vectors}
import org.mapdb.DBMaker.Maker
import org.mapdb.{DBMaker, HTreeMap, Serializer}

private[deploy] object ShardDatabase {

  var actors: Array[Seq[ActorRef]] = null

  @volatile var totalTime = 0L

  class InitializeWorker(parallelism: Int, lsh: LSH) extends Actor {

    context.setReceiveTimeout(20000 milliseconds)

    override def postStop(): Unit = {
      println("table building cost " + totalTime + " milliseconds, " +
        "vector count:" + vectorIdToVector.size())
    }

    override def receive: Receive = {
      case sv: SparseVector =>
        val startTime = System.currentTimeMillis()
        val bucketIndices = lsh.calculateIndex(sv)
        for (i <- 0 until bucketIndices.length) {
          //need to ensure that certain part of bucketIndices are accessible for single thread/actor
          actors(i)(bucketIndices(i) % parallelism) !
            Tuple3(i, bucketIndices(i), sv.vectorId)
        }
        totalTime += (System.currentTimeMillis() - startTime)
      case x @ (_, _, _) =>
        val startTime = System.currentTimeMillis()
        val table = vectorDatabase(x._1.asInstanceOf[Int])
        if (!table.containsKey(x._2)) {
          table.put(x._2.asInstanceOf[Int], new ListBuffer[Int])
        }
        val l = table.get(x._2)
        l += x._3.asInstanceOf[Int]
        totalTime += (System.currentTimeMillis() - startTime)
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
    def initializeVectorDatabase(): HTreeMap[Int, ListBuffer[Int]] =  {
      val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
        counterEnable().
        keySerializer(Serializer.INTEGER)
      hashMapMaker.make[Int, ListBuffer[Int]]()
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
      tableNum: Int,
      parallelism: Int): Unit = {
    actors = Array.fill[Seq[ActorRef]](tableNum)(null)
    for (i <- 0 until tableNum) {
      actors(i) = for (j <- 0 until parallelism) yield actorSystem.actorOf(
        Props(new InitializeWorker(parallelism, lsh)))
    }
    val allFiles = Utils.buildFileListUnderDirectory(filePath)

    for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
      val (id, size, indices, values) = Vectors.fromString1(line)
      val vector = new SparseVector(id, size, indices, values)
      vectorIdToVector.put(vector.vectorId, vector)
      actors(Random.nextInt(tableNum))(Random.nextInt(parallelism)) ! vector
    }
  }

  private[deploy] var vectorDatabase: Array[HTreeMap[Int, ListBuffer[Int]]] = null
  private[deploy] var vectorIdToVector: HTreeMap[Int, SparseVector] = null

  
}
