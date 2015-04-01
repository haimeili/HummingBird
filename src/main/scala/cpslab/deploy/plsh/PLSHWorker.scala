package cpslab.deploy.plsh

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.deploy.{SearchRequest, SimilarityIntermediateOutput}
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}
import cpslab.storage.{ByteArrayWrapper, LongBitSet}

private[plsh] class PLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {

  // vector storage
  private lazy val inMemoryTable = Array.fill(conf.getInt("cpslab.lsh.tableNum"))(
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[Int]])
  private val vectorIdToVector = new mutable.HashMap[Int, SparseVector]

  private val maxWorkerNumber = conf.getInt("cpslab.lsh.plsh.maxWorkerNum")
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")

  private val partitionSwitch = conf.getBoolean("cpslab.lsh.plsh.partitionSwitch")
  private lazy val inputFilePath = conf.getString("cpslab.lsh.inputFilePath")

  override def preStart(): Unit = {
    // TODO: think about whether we shall put reading table operation in preStart()
    // read files and save to the hash table
    initVectorStorage(inputFilePath)

  }

  private def initVectorStorage(filePath: String): Unit = {
    // TODO: change iterate over files to over vectors
    for (line <- Source.fromFile(filePath).getLines()) {
      try {
        val (size, indices, values, id) = Vectors.fromString(line)
        val vector = new SparseVector(id, size, indices, values)
        //partition data
        vectorIdToVector += vector.vectorId -> vector
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  private def partitionVectorTable(): Unit = {
    if (partitionSwitch) {
      // TODO: two-level partition

    } else {
      // general partition
      for (vector <- vectorIdToVector.map(_._2); i <- 0 until inMemoryTable.size) {
        saveVectorToSinglePartitionTable(vector, i)
      }
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
      inMemoryTable(tableId).getOrElseUpdate(vectorIndexInTable, new ListBuffer[Int]) +=
        vector.vectorId
    }
  }


  private def handleSearchRequestWithoutPartition(vector: SparseVector): Unit = {
    // save in local vector collection
    vectorIdToVector += vector.vectorId -> vector
    // PLSH needs to calculate similarity in all tables
    val indexOnAllTable = lshInstance.calculateIndex(vector)
    var similarVectors = List[(Int, Double)]()
    for (i <- 0 until indexOnAllTable.length) {
      val indexInCertainTable = indexOnAllTable(i)
      val keyInTable = ByteArrayWrapper(indexInCertainTable)
      // query the tables
      val similarityResultOpt = inMemoryTable(i).get(keyInTable).map(
        sparseVectors => sparseVectors.map {
          existingVectorId => (existingVectorId, SimilarityCalculator.fastCalculateSimilarity(
            vectorIdToVector(existingVectorId), vector))
        })
      val sortedSimilarityResult = similarityResultOpt.map(similarityResult =>
        similarityResult.sortWith { case ((id1, similarity1), (id2, similarity2)) =>
          similarity1 > similarity2
        }).map(list => if (topK > 0) list.take(topK) else list.filter(_._2 > similarityThreshold))
      sortedSimilarityResult.foreach(sortedSimilarVectorList =>
        similarVectors = similarVectors ++ sortedSimilarVectorList)
      // update the local tables
      saveVectorToSinglePartitionTable(vector, i, keyInTable)
    }
    if (similarVectors.size > 0) {
      sender ! SimilarityIntermediateOutput(vector.vectorId, new LongBitSet, similarVectors)
    }
  }

  private def handleSearchRequestWithPartition(vector: SparseVector): Unit = {

  }

  override def receive: Receive = {
    case SearchRequest(vector: SparseVector) =>
      if (partitionSwitch) {
        handleSearchRequestWithPartition(vector)
      } else {
        handleSearchRequestWithoutPartition(vector)
      }
  }
}


private[deploy] object PLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(id, conf, lshInstance))
  }
}
