package cpslab.deploy

import java.util.concurrent._

import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor._
import com.typesafe.config.{ConfigFactory, Config}
import cpslab.db._
import cpslab.deploy.benchmark.DataSetLoader
import cpslab.lsh.vector.SparseVector
import cpslab.utils.{HashPartitioner, Serializers}

private[cpslab] object ShardDatabase extends DataSetLoader {

  private def setupTable(tableName: String, confInstance: Config,
                         table: PartitionedHTreeMap[_, _]): Unit = {
    table.BUCKET_OVERFLOW = confInstance.getInt(s"cpslab.$tableName.bufferOverflow")
    table.updateBucketLength(confInstance.getInt(s"cpslab.$tableName.bucketBits"))
    table.updateDirectoryNodeSize(confInstance.getInt(s"cpslab.$tableName.dirNodeSize"),
      confInstance.getInt(s"cpslab.$tableName.chainLength"))
  }

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

  def initializeActorBasedHashTree(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lshTable.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new ActorBasedPartitionedHTreeMap[Int, Boolean](
            conf,
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
    def initializeIdToVectorMap(conf: Config): PartitionedHTreeMap[Int, SparseVector] =
      concurrentCollectionType match {
        case "Doraemon" =>
          new ActorBasedPartitionedHTreeMap[Int, SparseVector](
            conf,
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
    ActorBasedPartitionedHTreeMap.actorSystem = ActorSystem("AK", conf)
    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
      setupTable("lshTable", conf, vectorDatabase(tableId))
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector = initializeIdToVectorMap(conf)
    setupTable("mainTable", conf, vectorIdToVector)
    vectorIdToVector.initStructureLocks()
  }

  def initializeBTree(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val lockScale = conf.getInt("cpslab.lsh.btree.lockScale")
    val nodeSize = conf.getInt("cpslab.lsh.btree.nodeSize")
    val db = DBMaker.memoryUnsafeDB().transactionDisable().lockScale(lockScale).make()
    vectorIdToVectorBTree = db.treeMapCreate("vectorIdToVector").valuesOutsideNodesEnable().
      keySerializer(Serializers.IntSerializer).valueSerializer(Serializers.VectorSerializer).
      nodeSize(nodeSize).make[Int, SparseVector]()
    vectorDatabaseBTree = new Array[BTreeMap[Long, LSHBTreeVal]](tableNum)
    for (tableId <- 0 until tableNum) {
      val db1 = DBMaker.memoryUnsafeDB().transactionDisable().lockScale(lockScale).make()
      vectorDatabaseBTree(tableId) =
        db1.treeMapCreate(s"vectorDatabaseBTree - $tableId").valuesOutsideNodesEnable().
          keySerializer(Serializers.scalaLongSerializer).
          valueSerializer(Serializers.vectorIDHashPairSerializer).
          nodeSize(nodeSize).make[Long, LSHBTreeVal]()
      vectorDatabaseBTree(tableId).tableId = tableId
    }
  }

  def initializeMapDBHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
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
            new HashPartitioner[Int](numPartitions),
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
      setupTable("lshTable", conf, vectorDatabase(tableId))
    }
    vectorIdToVector = initializeIdToVectorMap()
    setupTable("mainTable", conf, vectorIdToVector)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector.initStructureLocks()
  }


  def initializePartitionedHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val concurrentCollectionType = conf.getString("cpslab.lsh.concurrentCollectionType")
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")

    // LSHTable configurations
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val ifFromFile = conf.getString("cpslab.lsh.partitionBitsGenerateMethod")
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lsh.generateMethod=$ifFromFile
         |cpslab.lsh.familyFilePath=partitionFunc.txt
         |cpslab.lshTable.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): PartitionedHTreeMap[Int, Boolean] =
      concurrentCollectionType match {
        case "Doraemon" =>
          val newTree = new ActorPartitionedHTreeBasic[Int, Boolean](
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
      setupTable("lshTable", conf, vectorDatabase(tableId))
    }

    vectorIdToVector = initializeIdToVectorMap()
    setupTable("mainTable", conf, vectorIdToVector)
    for (tableId <- 0 until tableNum) {
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector.initStructureLocks()
  }

  var vectorDatabase: Array[PartitionedHTreeMap[Int, Boolean]] = null
  var vectorIdToVector: PartitionedHTreeMap[Int, SparseVector] = null

  // (partial/complete) hash value -> (vectorId, complete hash value)
  var vectorDatabaseBTree: Array[BTreeMap[Long, LSHBTreeVal]] = null
  var vectorIdToVectorBTree: BTreeMap[Int, SparseVector] = null

  var vectorDatabaseOnheap: Array[PartitionedHTreeMapOnHeap[Int, Boolean]] = null
  var vectorIdToVectorOnheap: PartitionedHTreeMapOnHeap[Int, SparseVector] = null
}
