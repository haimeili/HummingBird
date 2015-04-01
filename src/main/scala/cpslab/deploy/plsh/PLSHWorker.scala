package cpslab.deploy.plsh

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.deploy.{SearchRequest, SimilarityIntermediateOutput}
import cpslab.lsh._
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}
import cpslab.storage.{ByteArrayWrapper, LongBitSet}

private[plsh] class PLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {

  // vector storage
  private lazy val singleLevelPartitionTable = Array.fill(conf.getInt("cpslab.lsh.tableNum"))(
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[Int]])
  private lazy val twoLevelPartitionTable = new mutable.HashMap[ByteArrayWrapper,
    mutable.HashMap[ByteArrayWrapper, mutable.HashSet[Int]]]()
  private lazy val partitionersInPrecalculatedChain = new ListBuffer[PrecalculatedHashChain]
  private val vectorIdToVector = new mutable.HashMap[Int, SparseVector]

  private val maxWorkerNumber = conf.getInt("cpslab.lsh.plsh.maxWorkerNum")
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")

  private val partitionSwitch = conf.getBoolean("cpslab.lsh.plsh.partitionSwitch")
  private lazy val inputFilePath = conf.getString("cpslab.lsh.inputFilePath")

  override def preStart(): Unit = {
    // TODO: think about whether we shall put reading table operation in preStart()
    // read files and save to the hash table
    if (partitionSwitch) {
      //check if it's the right setup
      for (hashChainForEachTable <- lshInstance.tableIndexGenerators) {
        assert(hashChainForEachTable.isInstanceOf[PrecalculatedHashChain])
        partitionersInPrecalculatedChain += hashChainForEachTable.
          asInstanceOf[PrecalculatedHashChain]
      }
    }
    initVectorStorage(inputFilePath)
  }

  private def initVectorStorage(filePath: String): Unit = {
    if (filePath != "") {
      // TODO: change iterate over files to over vectors
      for (line <- Source.fromFile(filePath).getLines()) {
        try {
          val (size, indices, values, id) = Vectors.fromString(line)
          val vector = new SparseVector(id, size, indices, values)
          //partition data
          partitionVectorTable(vector)
        } catch {
          case e: Exception =>
            e.printStackTrace()
        }
      }
    }
  }

  private def partitionVectorTable(vector: SparseVector): Unit = {
    if (partitionSwitch) {
      //two-level partition
      saveVectorToTwoLevelPartitionTable(vector)
    } else {
      // general partition
      for (i <- 0 until singleLevelPartitionTable.length) {
        saveVectorToSinglePartitionTable(vector, i)
      }
    }
  }

  /**
   * save vector to the table which was partitioned in two levels
   * @param vector the vector to be saved
   */
  private def saveVectorToTwoLevelPartitionTable(vector: SparseVector): Unit = {
    //put in first level
    val selectedPartitionIDs = new mutable.HashMap[Int, ByteArrayWrapper]
    for (partitioner <- partitionersInPrecalculatedChain) {
      val firstLevelIndex = selectedPartitionIDs.getOrElseUpdate(
        partitioner.firstPartitionerID,
        ByteArrayWrapper(partitioner.computeFirstLevelIndex(vector)))
      val secondLevelIndex = selectedPartitionIDs.getOrElseUpdate(
        partitioner.secondPartitionerID,
        ByteArrayWrapper(partitioner.computeSecondLevelIndex(vector)))
      twoLevelPartitionTable.
        getOrElseUpdate(firstLevelIndex,
          new mutable.HashMap[ByteArrayWrapper, mutable.HashSet[Int]]).
        getOrElseUpdate(secondLevelIndex, new mutable.HashSet[Int]) += vector.vectorId
    }
  }

  /**
   * save vector to the table which is only partitioned in one level,
   * if vectorIndexInTable is not provided, we need to recalculate the
   * index of the table, otherwise, we use the provided index directly
   * @param vector the vector to be saved
   * @param tableId target tableID
   * @param vectorIndexInTable the index of the vector in table
   */
  private def saveVectorToSinglePartitionTable(
      vector: SparseVector,
      tableId: Int,
      vectorIndexInTable: ByteArrayWrapper = null): Unit = {
    val index: ByteArrayWrapper = {
      if (vectorIndexInTable == null) {
        ByteArrayWrapper(lshInstance.calculateIndex(vector)(tableId))
      } else {
        vectorIndexInTable
      }
    }
    // update the local tables
    if (math.abs(index.hashCode()) % maxWorkerNumber == id) {
      vectorIdToVector += vector.vectorId -> vector
      singleLevelPartitionTable(tableId).getOrElseUpdate(vectorIndexInTable, new ListBuffer[Int]) +=
        vector.vectorId
    }
  }

  /**
   * process the search request when the vectors are saved in a table which is partitioned in
   * two levels
   * @param queryVector the query vector
   */
  private def handleSearchRequestWithTwoLevelParittionTable(queryVector: SparseVector): Unit = {
    val selectedPartitionIDs = new mutable.HashMap[ByteArrayWrapper,
      mutable.HashSet[ByteArrayWrapper]]
    for (partitioner <- partitionersInPrecalculatedChain) {
      val firstLevelIndex = ByteArrayWrapper(partitioner.computeFirstLevelIndex(queryVector))
      val secondLevelIndex = ByteArrayWrapper(partitioner.computeSecondLevelIndex(queryVector))
      selectedPartitionIDs.getOrElseUpdate(firstLevelIndex,
        new mutable.HashSet[ByteArrayWrapper]) += secondLevelIndex
    }
    // get all candidates
    val candidateIndices = new mutable.HashSet[Int]
    for ((firstLevelIndex, secondLevelIndices) <- selectedPartitionIDs) {
      val firstLevelSetOpt = twoLevelPartitionTable.get(firstLevelIndex)
      firstLevelSetOpt.foreach(firstLevelSet => {
        secondLevelIndices.foreach(secondLevelIdx =>
          firstLevelSet.get(secondLevelIdx).foreach(secondLevelSet =>
            candidateIndices ++= secondLevelSet)
        )
      })
    }
    val candidateVectors = candidateIndices.toList.map(vectorId => vectorIdToVector(vectorId)).map(
      sparseVector => (sparseVector.vectorId, SimilarityCalculator.fastCalculateSimilarity(
          sparseVector, queryVector))).sortWith((v1, v2) => v1._2 > v2._2)
    //return result
    if (candidateVectors.size > 0) {
      val bitmap = new LongBitSet
      candidateVectors.map(_._1).foreach(similarVectorIdx => bitmap.set(similarVectorIdx))
      sender ! SimilarityIntermediateOutput(queryVector.vectorId, bitmap, candidateVectors)
    }
  }

  /**
   * process the search request when the vectors are saved in a table which is only partitioned in
   * the first level
   * @param queryVector the query vector
   */
  private def handleSearchRequestWithSingleLevelPartition(queryVector: SparseVector): Unit = {
    // PLSH needs to calculate similarity in all tables
    val indexOnAllTable = lshInstance.calculateIndex(queryVector)
    var similarVectors = List[(Int, Double)]()
    for (i <- 0 until indexOnAllTable.length) {
      val indexInCertainTable = indexOnAllTable(i)
      val keyInTable = ByteArrayWrapper(indexInCertainTable)
      // query the tables
      val similarityResultOpt = singleLevelPartitionTable(i).get(keyInTable).map(
        sparseVectors => sparseVectors.map {
          existingVectorId => (existingVectorId, SimilarityCalculator.fastCalculateSimilarity(
            vectorIdToVector(existingVectorId), queryVector))
        })
      val sortedSimilarityResult = similarityResultOpt.map(similarityResult =>
        similarityResult.sortWith { case ((id1, similarity1), (id2, similarity2)) =>
          similarity1 > similarity2
        }).map(list => if (topK > 0) list.take(topK) else list.filter(_._2 > similarityThreshold))
      sortedSimilarityResult.foreach(sortedSimilarVectorList =>
        similarVectors = similarVectors ++ sortedSimilarVectorList)
      // update the local tables
      saveVectorToSinglePartitionTable(queryVector, i, keyInTable)
    }
    if (similarVectors.size > 0) {
      val bitmap = new LongBitSet
      similarVectors.map(_._1).foreach(similarVectorIdx => bitmap.set(similarVectorIdx))
      sender ! SimilarityIntermediateOutput(queryVector.vectorId, bitmap, similarVectors)
    }
  }

  override def receive: Receive = {
    case SearchRequest(vector: SparseVector) =>
      if (partitionSwitch) {
        handleSearchRequestWithTwoLevelParittionTable(vector)
      } else {
        handleSearchRequestWithSingleLevelPartition(vector)
      }
  }
}


private[deploy] object PLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(id, conf, lshInstance))
  }
}
