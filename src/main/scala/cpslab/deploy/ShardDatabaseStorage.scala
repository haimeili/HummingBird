package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import cpslab.storage.ByteArrayWrapper

private[deploy] class ShardDatabaseStorage(conf: Config) extends Actor {
  
  private val elementsInIndependentSpace = 
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]]
  private val elementsInFlatSpace =
    new mutable.HashMap[Int, mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]]]
  
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private val shardingNamespace = conf.getString("cpslab.lsh.sharding.namespace")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")
  
  private val clientActorAddress = conf.getString("cpslab.lsh.deploy.client")

  private val clientActor = {
    context.actorSelection(clientActorAddress)
  }

  private def handleIndexRequest(indexRequest: LSHTableIndexRequest) = shardingNamespace match {
    case "independent" =>
      for ((_, vectors) <- indexRequest.indexMap; vector <- vectors) {
        val bucketIndex = ByteArrayWrapper(
          vector.bucketIndex.filter(indexInTable => indexInTable != null)(0))
        //get all existing vectors
        val allVectorCandidates = elementsInIndependentSpace.get(bucketIndex)
        //calculate similarity
        val selectedCandidates = allVectorCandidates.map(allcandidates =>
          allcandidates.foldLeft(new ListBuffer[(String, Double)])
            ((selectedCandidates, candidate) => selectedCandidates +=
              candidate._1 -> SimilarityCalculator.calculateSimilarity(vector.sparseVector,
                candidate._2))
        )
        //send back to client
        selectedCandidates.foreach(candidatesList => {
          if (topK > 0) {
            clientActor ! SimilarityOutput(vector.vectorID,
              selectedCandidates.get.sortWith((a, b) => a._2 > b._2).take(topK).toList)
          } else {
            clientActor ! SimilarityOutput(vector.vectorID,
              selectedCandidates.get.filter(_._2 > similarityThreshold).toList)
          }
        })
        // save to elements
        elementsInIndependentSpace.getOrElseUpdate(bucketIndex, 
          new ListBuffer[(String, SparseVector)]) += (vector.vectorID -> vector.sparseVector)
      }
    case "flat" =>
      for ((tableId, vectors) <- indexRequest.indexMap; vector <- vectors) {
        val bucketIndex = ByteArrayWrapper(
          vector.bucketIndex.filter(indexInTable => indexInTable != null)(0))
        // get all existing vectors
        elementsInFlatSpace.getOrElseUpdate(tableId, 
          new mutable.HashMap[ByteArrayWrapper, ListBuffer[(String, SparseVector)]])
        val allVectorCandidates = elementsInFlatSpace(tableId).get(bucketIndex)
        // calculate similarity
        val selectedCandidates = allVectorCandidates.map(allcandidates =>
          allcandidates.foldLeft(new ListBuffer[(String, Double)])
            ((selectedCandidates, candidate) => selectedCandidates +=
              candidate._1 -> SimilarityCalculator.calculateSimilarity(vector.sparseVector,
                candidate._2))
        )
        //send back to client
        selectedCandidates.foreach(candidatesList => {
          if (topK > 0) {
            clientActor ! SimilarityOutput(vector.vectorID,
              selectedCandidates.get.sortWith((a, b) => a._2 > b._2).take(topK).toList)
          } else {
            clientActor ! SimilarityOutput(vector.vectorID,
              selectedCandidates.get.filter(_._2 > similarityThreshold).toList)
          }
        })
        // save to elements
        elementsInFlatSpace(tableId).getOrElseUpdate(bucketIndex, 
          new ListBuffer[(String, SparseVector)]) += (vector.vectorID -> vector.sparseVector)
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
