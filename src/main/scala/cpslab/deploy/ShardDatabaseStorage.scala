package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Props, Actor}
import com.typesafe.config.Config
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import cpslab.storage.ByteArrayWrapper

private[deploy] class ShardDatabaseStorage(conf: Config) extends Actor {
  
  private val elements = new mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]]
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")
  
  private val clientActorAddress = conf.getString("cpslab.lsh.deploy.client")

  private val clientActor = {
    context.actorSelection(clientActorAddress)
  }

  override def receive: Receive = {
    case indexRequest @ LSHTableIndexRequest(_) =>
      for ((shardID, vectors) <- indexRequest.indexMap; vector <- vectors) {
        val bucketIndex = ByteArrayWrapper(
          vector.bucketIndex.filter(indexInTable => indexInTable != null)(0))
        //get all existing vectors
        val allVectorCandidates = elements.get(bucketIndex)
        //calculate similarity
        val selectedCandidates = allVectorCandidates.map(allcandidates =>
          allcandidates.foldLeft(new ListBuffer[(String, Double)])
            ((selectedCandidates, candidate) => selectedCandidates +=
              candidate._1 -> SimilarityCalculator.calculateSimilarity(vector.sparseVector, 
              candidate._2))
        )
        //send back to client
        selectedCandidates.map(candidatesList => {
          if (topK > 0) {
            clientActor ! SimilarityOutput(vector.vectorID,
              selectedCandidates.get.sortWith((a, b) => a._2 > b._2).take(topK).toList)
          } else {
            clientActor ! SimilarityOutput(vector.vectorID,
              selectedCandidates.get.filter(_._2 > similarityThreshold).toList)
          }
        })
        // save to elements
        elements.getOrElseUpdate(bucketIndex, new ListBuffer[(String, SparseVector)]) +=
          (vector.vectorID -> vector.sparseVector)
      }
  }
}

private[deploy] object ShardDatabaseStorage {
  def props(conf: Config): Props = Props(new ShardDatabaseStorage(conf))
}
