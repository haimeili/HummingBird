package cpslab.deploy.utils

import scala.collection.mutable

import akka.actor.Actor
import cpslab.deploy.SimilarityOutput

private[deploy] class Client extends Actor {
  val state = new mutable.HashMap[Int, mutable.HashSet[Int]]

  override def receive = {
    case SimilarityOutput(vectorId, bitmap, similarIDs) =>
      for (similarId <- similarIDs) {
        state.getOrElseUpdate(vectorId, new mutable.HashSet[Int]) += similarId._1
      }
  }
}
