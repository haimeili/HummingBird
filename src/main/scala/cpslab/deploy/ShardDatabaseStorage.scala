package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.lsh.vector.{SparseVectorWrapper, SimilarityCalculator, SparseVector}
import cpslab.storage.ByteArrayWrapper

private[deploy] class ShardDatabaseStorage(conf: Config) extends Actor {

  // data structures for different sharding schema
  private lazy val elementsInIndependentSpace =
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]]
  private lazy val elementsInFlatSpace =
    new mutable.HashMap[Int,
      mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]]]
  
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private val shardingNamespace = conf.getString("cpslab.lsh.sharding.namespace")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")
  
  private val clientActorAddress = conf.getString("cpslab.lsh.deploy.client")

  private val clientActor = {
    context.actorSelection(clientActorAddress)
  }

  private def generateSimilarityOutput(
      vectorID: String,
      vector: SparseVector,
      candidateList: Option[ListBuffer[(String, SparseVector)]]): Option[SimilarityOutput] = {
    //calculate similarity
    val selectedCandidates = candidateList.map(allcandidates =>
      allcandidates.foldLeft(new ListBuffer[(String, Double)])
        ((selectedCandidates, candidate) => selectedCandidates +=
          candidate._1 -> SimilarityCalculator.fastCalculateSimilarity(vector,
            candidate._2))
    )
    selectedCandidates.map(candidates =>
      if (topK > 0) {
        SimilarityOutput(vectorID,
          candidates.sortWith((a, b) => a._2 > b._2).take(topK).toList)
      } else {
        SimilarityOutput(vectorID, candidates.filter(_._2 > similarityThreshold).toList)
      }
    )
  }

  private def handleIndexRequest(indexRequest: LSHTableIndexRequest): Unit = {
    for ((tableId, vectors) <- indexRequest.indexMap; vector <- vectors) {
      val bucketIndex = ByteArrayWrapper(
        vector.bucketIndex.filter(indexInTable => indexInTable != null)(0))
      val allVectorCandidates = shardingNamespace match {
        case "independent" =>
          elementsInIndependentSpace.get(bucketIndex)
        case "flat" =>
          elementsInFlatSpace.getOrElseUpdate(tableId,
            new mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]])
          elementsInFlatSpace(tableId).get(bucketIndex)
      }
      val simOutputOpt = generateSimilarityOutput(
        vectorID = vector.vectorID,
        vector = vector.sparseVector,
        candidateList = allVectorCandidates)
      simOutputOpt.foreach(simOutput => clientActor ! simOutput)
      // index the vector
      indexVector(tableId, bucketIndex, vector.vectorID, vector.sparseVector)
    }

  }

  private def indexVector(
      tableId: Int,
      bucketIndex: ByteArrayWrapper,
      vectorID: String,
      vector: SparseVector): Unit = {
    shardingNamespace match {
      case "independent" =>
        elementsInIndependentSpace.getOrElseUpdate(bucketIndex,
          new ListBuffer[(String, SparseVector)]) += (vectorID -> vector)
      case "flat" =>
        elementsInFlatSpace(tableId).getOrElseUpdate(bucketIndex,
          new ListBuffer[(String, SparseVector)]) += (vectorID -> vector)
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
