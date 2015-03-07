package cpslab.deploy.plsh

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.deploy.{SearchRequest, SimilarityOutput}
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import cpslab.storage.ByteArrayWrapper

private[plsh] class PLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {
  
  private val inMemoryTable = Array.fill(conf.getInt("cpslab.lsh.tableNum"))(
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[String]])
  
  private val vectorIdToVector = new mutable.HashMap[String, SparseVector]
  private val maxWorkerNumber = conf.getInt("cpslab.lsh.plsh.maxWorkerNum")
  
  override def receive: Receive = {
    case SearchRequest(vectorId: String, vector: SparseVector, topK: Int) =>
      // PLSH needs to calculate similarity in all tables
      val indexOnAllTable = lshInstance.calculateIndex(vector)
      var similarVectors = List[(String, Double)]()
      for (i <- 0 until indexOnAllTable.size) {
        val indexInCertainTable = indexOnAllTable(i)
        val keyInTable = ByteArrayWrapper(indexInCertainTable)
        // query the tables
        val similarityResultOpt = inMemoryTable(i).get(keyInTable).map(
          sparseVectors => sparseVectors.map {
            existingVectorId => (existingVectorId, SimilarityCalculator.calculateSimilarity(
              vectorIdToVector(existingVectorId), vector))
          })
        val sortedSimilarityResult = similarityResultOpt.map(similarityResult =>
          similarityResult.sortWith { case ((id1, similarity1), (id2, similarity2)) =>
            similarity1 > similarity2
          }).map(list => if (topK > 0) list.take(topK) else list)
        sortedSimilarityResult.foreach(sortedSimilarVectorList => 
          similarVectors = similarVectors ++ sortedSimilarVectorList)
        // update the local tables
        if (math.abs(keyInTable.hashCode()) % maxWorkerNumber == id) {
          vectorIdToVector += vectorId -> vector
          inMemoryTable(i).getOrElseUpdate(keyInTable, new ListBuffer[String]) += vectorId
        }
      }
      if (similarVectors.size > 0) {
        sender ! SimilarityOutput(vectorId, Some(similarVectors))
      }
  }
}


private[deploy] object PLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(id, conf, lshInstance))
  }
}
