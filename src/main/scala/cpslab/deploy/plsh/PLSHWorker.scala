package cpslab.deploy.plsh

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable.ParMap
import scala.collection.parallel.mutable.ParIterable
import scala.concurrent.ExecutionContext
import scala.io.Source

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.deploy.plsh.PLSHExecutionContext._
import cpslab.deploy.{SearchRequest, SimilarityIntermediateOutput}
import cpslab.lsh._
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}
import cpslab.storage.ByteArrayWrapper

private[plsh] class PLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {

  // general setup
  private val tableNum = conf.getInt("cpslab.lsh.tableNum")
  private val maxWorkerNumber = conf.getInt("cpslab.lsh.plsh.maxWorkerNum")
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")
  private lazy val inputFilePath = conf.getString("cpslab.lsh.inputFilePath")

  // vector storage
  //two-level partition
  // 2d array
  // tableID => (vectorID, bucketIndex)
  private[plsh] val twoLevelPartitionTable =
    Array.fill[Array[(Int, ByteArrayWrapper)]](tableNum)(null)
  private[plsh] var bucketOffsetTable: Array[ParMap[ByteArrayWrapper, Int]] = null

  private lazy val partitionersInPrecalculatedChain = new ListBuffer[PrecalculatedHashChain]
  private val vectorIdToVector = new mutable.HashMap[Int, SparseVector]
    with mutable.SynchronizedMap[Int, SparseVector]

  override def preStart(): Unit = {
    // TODO: whether we shall put reading table operation in preStart()
    // read files and save to the hash table
    //check if it's the right setup
    for (hashChainForEachTable <- lshInstance.tableIndexGenerators) {
      assert(hashChainForEachTable.isInstanceOf[PrecalculatedHashChain])
      partitionersInPrecalculatedChain += hashChainForEachTable.
        asInstanceOf[PrecalculatedHashChain]
    }
    initVectorStorage(inputFilePath)
  }

  private def initVectorStorage(filePath: String)(implicit executorService: ExecutionContext):
      Unit = {
    if (filePath != "") {
      // read all files
      for (line <- Source.fromFile(filePath).getLines()) {
        val (size, indices, values, id) = Vectors.fromString(line)
        val vector = new SparseVector(id, size, indices, values)
        vectorIdToVector += vector.vectorId -> vector
      }
      // initialize tables
      for (i <- 0 until tableNum) {
        twoLevelPartitionTable(i) = new Array[(Int, ByteArrayWrapper)](vectorIdToVector.size)
      }
      // calculate the bucket index for all vectors in all tables
      val bucketIndexOfAllVectors: ParIterable[Array[(ByteArrayWrapper, Int)]] =
        vectorIdToVector.par.map { case (vectorId, sparseVector) =>
          lshInstance.calculateIndex(sparseVector).map(bucketIndex =>
            (ByteArrayWrapper(bucketIndex), sparseVector.vectorId))
        }
      // calculate the offset of each bucket in all tables
      bucketOffsetTable = calculateOffsetofAllBuckets(bucketIndexOfAllVectors)
      // calculate offset of each vector
      calculateOffSetForAllVectors(bucketIndexOfAllVectors)
    }
  }

  private def calculateOffsetofAllBuckets(
      bucketIndexOfAllVectors: ParIterable[Array[(ByteArrayWrapper, Int)]]):
      Array[ParMap[ByteArrayWrapper, Int]] = {
    val tableIndex = new AtomicInteger(0)
    val tempTable = Array.fill[Array[(ByteArrayWrapper, Int)]](tableNum)(null)
    for (bucketIndexToVector <- bucketIndexOfAllVectors) {
      tempTable(tableIndex.getAndIncrement) = bucketIndexToVector
    }
    tempTable.map(table =>
      table.par.groupBy(_._1).map{case (bucketIndex, array) => (bucketIndex, array.size)})
  }

  private def calculateOffSetForAllVectors(
      bucketIndexOfAllVectors: ParIterable[Array[(ByteArrayWrapper, Int)]]): Unit = {

    assert(bucketOffsetTable != null)

    val tableIndex = new AtomicInteger(0)
    tableIndex.set(0)
    val inBucketOffsetArray =
      Array.fill[mutable.HashMap[ByteArrayWrapper, AtomicInteger]](tableNum)(null)
    //initialize the offset counter for each bucket in all tables
    for (i <- 0 until tableNum) {
      bucketOffsetTable(i).foreach{case (bucketIndex, offset) =>
        inBucketOffsetArray(i) += bucketIndex -> new AtomicInteger(0)}
    }
    for (bucketIndexToVector <- bucketIndexOfAllVectors) {
      bucketIndexToVector.par.foreach{case (bucketIndex, vectorId) =>
        // compute offset
        val offset = bucketOffsetTable(tableIndex.get())(bucketIndex) +
          inBucketOffsetArray(tableIndex.get())(bucketIndex).getAndIncrement
        twoLevelPartitionTable(tableIndex.get())(offset) = (vectorId, bucketIndex)
      }
    }
  }

  /**
   * process the search request when the vectors are saved in a table which is partitioned in
   * two levels
   * @param queryVector the query vector
   */
  private def handleSearchRequestWithTwoLevelParittionTable(queryVector: SparseVector): Unit = {
    val similarCandidates = new util.BitSet
    val queryIndexInAllTable = lshInstance.calculateIndex(queryVector)
    for (i <- 0 until tableNum) {
      val bucketIndex = ByteArrayWrapper(queryIndexInAllTable(i))
      // initialize the vector offset to be the bucketoffset
      if (bucketOffsetTable != null) {
        val vectorOffsetOpt = bucketOffsetTable(i).get(bucketIndex)
        vectorOffsetOpt.foreach(vectorOffset => {
          var vectorOffsetWorkerPointer = vectorOffset
          var candidateVectorAndBucketIndex = twoLevelPartitionTable(i)(vectorOffset)
          while (candidateVectorAndBucketIndex._2 == bucketIndex) {
            similarCandidates.set(vectorIdToVector(candidateVectorAndBucketIndex._1).vectorId)
            vectorOffsetWorkerPointer += 1
            candidateVectorAndBucketIndex = twoLevelPartitionTable(i)(vectorOffset)
          }
        })
      }
    }
    if (similarCandidates.cardinality() > 0) {
      var nextSimilarVectorID = similarCandidates.nextSetBit(0)
      val similarVectors = new ListBuffer[(Int, Double)]
      while (nextSimilarVectorID >= 0) {
        similarVectors += nextSimilarVectorID -> SimilarityCalculator.fastCalculateSimilarity(
          vectorIdToVector(nextSimilarVectorID), queryVector)
        nextSimilarVectorID = similarCandidates.nextSetBit(nextSimilarVectorID + 1)
      }
      //send
      sender ! SimilarityIntermediateOutput(queryVector.vectorId, null, similarVectors.toList)
    }
  }

  private def handleSearchRequest(vector: SparseVector)
      (implicit executorService: ExecutionContext): Unit = {
    executorService.execute(new Runnable {
      override def run(): Unit = {
        handleSearchRequestWithTwoLevelParittionTable(vector)
      }
    })
  }

  override def receive: Receive = {
    case SearchRequest(vector: SparseVector) =>
      handleSearchRequest(vector)
  }
}

private[deploy] object PLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(id, conf, lshInstance))
  }
}
