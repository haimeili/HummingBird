package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import cpslab.storage.LongBitSet

private[deploy] class ShardDatabaseStorage(conf: Config) extends Actor {

  // data structures for different sharding schema
  private[deploy] lazy val elementsInIndependentSpace =
    new mutable.HashMap[Int, ListBuffer[SparseVector]]
  private[deploy] lazy val elementsInFlatSpace =
    new mutable.HashMap[Int, mutable.HashMap[Int, ListBuffer[SparseVector]]]
  
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private val shardingNamespace = conf.getString("cpslab.lsh.sharding.namespace")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")

  private val writeActorsNum = conf.getInt("cpslab.lsh.writerActorNum")
  
  private def generateSimilarityOutput(
      vectorID: Int,
      vector: SparseVector,
      candidateList: Option[ListBuffer[SparseVector]]): Option[SimilarityIntermediateOutput] = {
    if (!candidateList.isDefined) {
      Some(SimilarityIntermediateOutput(vectorID, new LongBitSet, List[(Int, Double)]()))
    } else {
      //calculate similarity
      val selectedCandidates = candidateList.map(allCandidates =>
        allCandidates.foldLeft(new ListBuffer[(Int, Double)])
          ((selectedCandidates, candidate) => selectedCandidates +=
            candidate.vectorId -> SimilarityCalculator.fastCalculateSimilarity(vector, candidate))
      )
      selectedCandidates.map(candidates => {
        val result = candidates.filter(_._2 > similarityThreshold).sortWith((a, b) => a._2 > b._2).
          take(topK).toList
        val bitmap = new LongBitSet
        result.foreach(pair => bitmap.set(pair._1))
        SimilarityIntermediateOutput(vectorID, bitmap, result)
      }
      )
    }
  }

  private def handleIndexRequest(indexRequest: LSHTableIndexRequest): Unit = {
    for ((tableId, vectors) <- indexRequest.indexMap; vector <- vectors) {
      val allVectorCandidates = new ListBuffer[SparseVector]

      for (bucketIndex <- vector.bucketIndex) {
        val candidates = shardingNamespace match {
          case "independent" =>
            elementsInIndependentSpace.get(bucketIndex)
          case "flat" =>
            elementsInFlatSpace.getOrElseUpdate(tableId,
              new mutable.HashMap[Int, ListBuffer[SparseVector]])
            elementsInFlatSpace(tableId).get(bucketIndex)
        }
        candidates.foreach(vectors => allVectorCandidates ++= vectors)
      }

      val simOutputOpt = generateSimilarityOutput(
        vectorID = vector.sparseVector.vectorId,
        vector = vector.sparseVector,
        candidateList = Some(allVectorCandidates))

      simOutputOpt.foreach(simOutput => {
          val outputActor = context.actorSelection(
            s"/user/writerActor-${simOutput.queryVectorID % writeActorsNum}")
          outputActor ! simOutput
        }
      )
      // index the vector
      for (bucketIndex <- vector.bucketIndex) {
        writeActorToTable(tableId, bucketIndex, vector.sparseVector.vectorId, vector.sparseVector)
      }
    }
  }

  private def writeActorToTable(
      tableId: Int,
      bucketIndex: Int,
      vectorID: Int,
      vector: SparseVector): Unit = {
    shardingNamespace match {
      case "independent" =>
        elementsInIndependentSpace.getOrElseUpdate(bucketIndex,
          new ListBuffer[SparseVector]) += vector
      case "flat" =>
        elementsInFlatSpace(tableId).getOrElseUpdate(bucketIndex,
          new ListBuffer[SparseVector]) += vector
    }
  }
  
  override def receive: Receive = {
    case indexRequest @ LSHTableIndexRequest(_) =>
      handleIndexRequest(indexRequest)
  }
}

private[deploy] object ShardDatabaseStorage {
  def props(conf: Config): Props = Props(new ShardDatabaseStorage(conf))
}
