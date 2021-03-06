package cpslab.deploy.benchmark

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import cpslab.lsh.vector.SparseVector

class VolatileHashMap {

  @volatile var totalCount = 0

  class WriterActor extends Actor {

    override def receive: Receive = {
      case Tuple2(key: Int, value: SparseVector) =>
        store.getOrElseUpdate(key, new ListBuffer[SparseVector]) += value
        totalCount += 1
    }
  }

  val store = new mutable.HashMap[Int, ListBuffer[SparseVector]]

  val actorSystem = ActorSystem("concurrentDSTest", ConfigFactory.parseString(
    """
      |akka.actor.provider=akka.actor.LocalActorRefProvider
    """.stripMargin))

  val writer = actorSystem.actorOf(Props(new WriterActor))

  def get(key: Int): Unit = {
    val a = totalCount
    store.get(key)
  }

  def put(key: Int, value: SparseVector) = {
    writer ! Tuple2(key, value)
  }
}
