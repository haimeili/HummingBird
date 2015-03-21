package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.Actor
import com.typesafe.config.Config
import cpslab.storage.LongBitSet

/**
 * class performing the functionality of output similar vectors to the client
 * NOTE: deduplicate the similarity
 */
private[deploy] class SimilarityOutputWriter(conf: Config) extends Actor {

  private lazy val receivedOutput = new mutable.HashMap[Int, Int]
  private lazy val maxDatabaseNodeNum = conf.getInt("cpslab.lsh.sharding.maxDatabaseNodeNum")

  private val similarPairs = new mutable.HashMap[Int, ListBuffer[(Int, Double)]]
  private val topK = conf.getInt("cpslab.lsh.topK")
  private val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")

  private val clientActorAddress = conf.getString("cpslab.lsh.deploy.client")

  private val clientActor = {
    context.actorSelection(clientActorAddress)
  }

  override def receive: Receive = {
    case SimilarityIntermediateOutput(queryID, bitmap, pairs) =>
      for ((id, similarity) <- pairs) {
        similarPairs.getOrElseUpdate(queryID, new ListBuffer[(Int, Double)]) += id -> similarity
      }
      receivedOutput.getOrElseUpdate(queryID, 0)
      receivedOutput(queryID) += 1
      if (receivedOutput(queryID) == maxDatabaseNodeNum && similarPairs.contains(queryID)) {
        //output
        val outputPairs = similarPairs(queryID).filter(_._2 > similarityThreshold).
          sortWith((a, b) => a._2 > b._2).take(topK)
        val bitmap = new LongBitSet
        outputPairs.foreach { case (id, similarity) => bitmap.set(id) }
        clientActor ! SimilarityOutput(queryID, bitmap, outputPairs.toList)
        receivedOutput.remove(queryID)
        similarPairs.remove(queryID)
      }
  }

}
