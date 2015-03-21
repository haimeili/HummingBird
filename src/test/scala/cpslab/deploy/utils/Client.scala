package cpslab.deploy.utils

import scala.collection.mutable

import akka.actor.Actor
import cpslab.deploy.SimilarityOutput

private[deploy] class Client extends Actor {
  val state = new mutable.HashMap[Int, Array[Long]]

  override def receive = {
    case SimilarityOutput(vectorId, bitmap, similarIDs) =>
      state.getOrElseUpdate(vectorId, similarIDs.map(_._1.toLong).toArray)
  }
}
