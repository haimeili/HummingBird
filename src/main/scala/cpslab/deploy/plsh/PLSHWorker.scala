package cpslab.deploy.plsh

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.deploy.{SearchRequest, SimilarityIntermediateOutput}
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import cpslab.storage.{ByteArrayWrapper, LongBitSet}

private[plsh] class PLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {
  
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

  }

  private def initializeVectorTable(filePath: String): Unit = {
    // TODO: change iterate over files to over vectors

  }

  private def handleSearchRequestWithoutPartition(vector: SparseVector): Unit = {
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
      if (math.abs(keyInTable.hashCode()) % maxWorkerNumber == id) {
        vectorIdToVector += vector.vectorId -> vector
        inMemoryTable(i).getOrElseUpdate(keyInTable, new ListBuffer[Int]) += vector.vectorId
      }
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
