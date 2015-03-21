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
  
  private val inMemoryTable = Array.fill(conf.getInt("cpslab.lsh.tableNum"))(
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[Int]])
  
  private val vectorIdToVector = new mutable.HashMap[Int, SparseVector]
  private val maxWorkerNumber = conf.getInt("cpslab.lsh.plsh.maxWorkerNum")
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")
  
  override def receive: Receive = {
    case SearchRequest(vectorId: Int, vector: SparseVector) =>
      // PLSH needs to calculate similarity in all tables
      val indexOnAllTable = lshInstance.calculateIndex(vector)
      var similarVectors = List[(Int, Double)]()
      for (i <- 0 until indexOnAllTable.size) {
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
          vectorIdToVector += vectorId -> vector
          inMemoryTable(i).getOrElseUpdate(keyInTable, new ListBuffer[Int]) += vectorId
        }
      }
      if (similarVectors.size > 0) {
        sender ! SimilarityIntermediateOutput(vectorId, new LongBitSet, similarVectors)
      }
  }
}


private[deploy] object PLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(id, conf, lshInstance))
  }
}
