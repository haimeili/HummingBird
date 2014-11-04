package cpslab.deploy

import akka.actor.{ActorLogging, Actor}
import akka.contrib.pattern.DistributedPubSubMediator.Publish
import akka.contrib.pattern.{DistributedPubSubExtension, ClusterSharding}

class Client extends Actor with ActorLogging {

  val mediator = DistributedPubSubExtension(context.system).mediator

  val postRegion = ClusterSharding(context.system).shardRegion(WorkerActor.shardName)

  def receive: Receive = {
    case q @ QueryRequest(_, _) =>
      mediator ! Publish("query", q)
    case i @ InsertRequest(_, _) =>
      mediator ! Publish("insert", i)
    case QueryResponse(responses) =>
      //TODO: get the response
  }
}

object Client {

  def main(args: Array[String]): Unit = {
    //TODO: main function
  }
}
