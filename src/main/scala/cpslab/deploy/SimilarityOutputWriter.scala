package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.Actor
import akka.actor.Actor.Receive

/**
 * class performing the functionality of output similar vectors to the client
 * NOTE: deduplicate the similarity
 */
private[deploy] class SimilarityOutputWriter extends Actor {
  private val outputBuffer = new mutable.HashMap[String, ListBuffer[(String, Double)]]
  override def receive: Receive = ???
}
