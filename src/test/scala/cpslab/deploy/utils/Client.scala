package cpslab.deploy.utils

import scala.collection.mutable

import akka.actor.Actor
import cpslab.deploy.SimilarityOutput

private[deploy] class Client extends Actor {
  val state = new mutable.HashMap[Int, List[(Int, Double)]]

  override def receive = {
    case SimilarityOutput(vectorId, similarVectors) =>
      state.getOrElseUpdate(vectorId, similarVectors)
  }
}
