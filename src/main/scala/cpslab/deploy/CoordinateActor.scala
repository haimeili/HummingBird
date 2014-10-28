package cpslab.deploy

import _root_.faimdata.similarity.all.ExecutorAddressNotification
import akka.actor.{Address, ActorSelection, Actor}
import akka.actor.Actor.Receive
import akka.remote.AssociationErrorEvent
import org.apache.spark.mllib.linalg.SparseVector

import scala.collection.mutable.{HashMap, ListBuffer}

class CoordinateActor(hashTableNum: Int) extends Actor {

  private val workerAddresses = new Array[String](hashTableNum)
  private val workerActors = new Array[ActorSelection](hashTableNum)
  private val addressToWorkerAddress = new HashMap[Address, Actor]()

  context.system.eventStream.subscribe(self,
    akka.remote.AssociatedEvent.getClass)

  override def receive: Receive = {
    case Register(execId, url) =>
      workerAddresses(execId) = url
      workerActors(execId) = context.system.actorSelection(url)
      println("Executor %d is registered at %s".format(execId, url))
    case AssociationErrorEvent =>
      // TODO: detect the association error
    case QueryRequest(vector: SparseVector) =>
      for (remoteWorkerActor <- workerActors) {
        remoteWorkerActor ! QueryRequest(vector: SparseVector)
      }
    case QueryResponse(vector: Array[SparseVector]) =>
      // TODO: provide forward to the client or provide some APIs with Await.result()
    case Insert(vector: SparseVector) =>
      // TODO: insert logic
  }
}
