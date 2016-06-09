package cpslab.deploy

import java.util.concurrent._

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.{ConfigFactory, Config}
import cpslab.db._
import cpslab.deploy.benchmark.DataSetLoader
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector
import cpslab.utils.{LocalitySensitivePartitioner, HashPartitioner, Serializers}

private[cpslab] object ShardDatabase extends DataSetLoader {

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
      case vectorId: Int =>
        if (hasSentReport) {
          monitor ! Report
          hasSentReport = false
        }
        if (startTime == -1L) {
          startTime = System.currentTimeMillis()
        }
        for (i <- vectorDatabase.indices) {
          vectorDatabase(i).put(vectorId, true)
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

  def initializeMapDBHashMapOnHeap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.lsh.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMapOnHeap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new PartitionedHTreeMapOnHeap[Int, Boolean](
            tableId,
            "lsh",
            workingDirRoot + "-" + tableId,
            "partitionedTree-" + tableId,
            new HashPartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            null,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
          newTree
      }
    def initializeIdToVectorMap(): PartitionedHTreeMapOnHeap[Int, SparseVector] =
      concurrentCollectionType match {
        case "Doraemon" =>
          new PartitionedHTreeMapOnHeap(
            tableNum,
            "default",
            workingDirRoot + "-vector",
            "vectorIdToVector",
            new HashPartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            Serializers.vectorSerializer,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
      }
    vectorDatabaseOnheap = new Array[PartitionedHTreeMapOnHeap[Int, Boolean]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabaseOnheap(tableId) = initializeVectorDatabase(tableId)
    }
    vectorIdToVectorOnheap = initializeIdToVectorMap()
  }

  def initializeBTree(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val lockScale = conf.getInt("cpslab.lsh.btree.lockScale")
    val nodeSize = conf.getInt("cpslab.lsh.btree.nodeSize")
    val db = DBMaker.memoryUnsafeDB().transactionDisable().lockScale(lockScale).make()

    vectorIdToVectorBTree = db.treeMapCreate("vectorIdToVector").keySerializer(
      Serializers.IntSerializer).valueSerializer(Serializers.VectorSerializer).nodeSize(nodeSize).
      make[Int, SparseVector]()
    vectorDatabaseBTree = new Array[BTreeMap[Int, Int]](tableNum)
    for (tableId <- 0 until tableNum) {
      val db1 = DBMaker.memoryUnsafeDB().lockScale(lockScale).make()
      vectorDatabaseBTree(tableId) =
        db1.treeMapCreate(s"vectorDatabaseBTree - $tableId").keySerializer(
          Serializers.IntSerializer).valueSerializer(Serializers.IntSerializer).nodeSize(nodeSize).
          make[Int, Int]()
    }
    /*
    vectorIdToVectorBTree = db.treeMap("vectorIdToVector",
      Serializers.IntSerializer, Serializers.VectorSerializer)
    vectorDatabaseBTree = new Array[BTreeMap[Int, Int]](tableNum)
    for (tableId <- 0 until tableNum) {
      val db1 = DBMaker.memoryUnsafeDB().transactionDisable().lockScale(lockScale).make()
      vectorDatabaseBTree(tableId) = db1.treeMap(s"vectorDatabaseBTree - $tableId",
        Serializers.IntSerializer, Serializers.IntSerializer)
    }*/
  }

  def initializeMapDBHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.lsh.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val dirNodeSize = conf.getInt("cpslab.lsh.htree.dirNodeSize")
    val bucketBits = conf.getInt("cpslab.lsh.bucketBits")
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new PartitionedHTreeMap[Int, Boolean](
            tableId,
            "lsh",
            workingDirRoot + "-" + tableId,
            "partitionedTree-" + tableId,
            new LocalitySensitivePartitioner[Int](confForPartitioner, tableId, partitionBits),
            true,
            1,
            Serializers.scalaIntSerializer,
            null,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold, true)
          newTree
      }
    def initializeIdToVectorMap(): PartitionedHTreeMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "Doraemon" =>
          new PartitionedHTreeMap[Int, SparseVector](
            tableNum,
            "default",
            workingDirRoot + "-vector",
            "vectorIdToVector",
            new HashPartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            Serializers.vectorSerializer,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold, true)
      }
    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
    }
    vectorIdToVector = initializeIdToVectorMap()
    PartitionedHTreeMap.BUCKET_OVERFLOW = conf.getInt("cpslab.bufferOverflow")
    PartitionedHTreeMap.updateBucketLength(bucketBits)
    PartitionedHTreeMap.updateDirectoryNodeSize(dirNodeSize)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector.initStructureLocks()
  }


  def initializePartitionedHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.lsh.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val dirNodeSize = conf.getInt("cpslab.lsh.htree.dirNodeSize")
    val bucketBits = conf.getInt("cpslab.lsh.bucketBits")
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new ActorPartitionedHTreeBasic[Int, Boolean](
            tableId,
            "lsh",
            workingDirRoot + "-" + tableId,
            "partitionedTree-" + tableId,
            new LocalitySensitivePartitioner[Int](confForPartitioner, tableId, partitionBits),
            true,
            1,
            Serializers.scalaIntSerializer,
            null,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
          newTree
      }
    def initializeIdToVectorMap(): PartitionedHTreeMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "Doraemon" =>
          new ActorPartitionedHTreeBasic[Int, SparseVector](
            tableNum,
            "default",
            workingDirRoot + "-vector",
            "vectorIdToVector",
            new HashPartitioner[Int](numPartitions),
            true,
            1,
            Serializers.scalaIntSerializer,
            Serializers.vectorSerializer,
            null,
            Executors.newCachedThreadPool(),
            true,
            ramThreshold)
      }
    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
    }
    vectorIdToVector = initializeIdToVectorMap()
    PartitionedHTreeMap.BUCKET_OVERFLOW = conf.getInt("cpslab.bufferOverflow")
    PartitionedHTreeMap.updateBucketLength(bucketBits)
    PartitionedHTreeMap.updateDirectoryNodeSize(dirNodeSize)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector.initStructureLocks()
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
    initVectorDatabaseFromFS(filePath, cap, tableNum)
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
            for (i <- vectorDatabase.indices) {
              totalCnt += vectorDatabase(i).size()
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
    for (i <- 0 until vectorIdToVector.getMaxPartitionNumber) {
      val itr = vectorIdToVector.values(i).iterator()
      while (itr.hasNext) {
        val vector = itr.next()
        actors(vector.vectorId % parallelism) ! vector.vectorId
      }
    }
  }

  var vectorDatabase: Array[PartitionedHTreeMap[Int, Boolean]] = null
  var vectorIdToVector: PartitionedHTreeMap[Int, SparseVector] = null

  var vectorDatabaseBTree: Array[BTreeMap[Int, Int]] = null
  var vectorIdToVectorBTree: BTreeMap[Int, SparseVector] = null

  var vectorDatabaseOnheap: Array[PartitionedHTreeMapOnHeap[Int, Boolean]] = null
  var vectorIdToVectorOnheap: PartitionedHTreeMapOnHeap[Int, SparseVector] = null
}
