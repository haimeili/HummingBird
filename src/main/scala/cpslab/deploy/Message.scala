package cpslab.deploy

import org.apache.spark.mllib.linalg.SparseVector

sealed trait Message extends Serializable

// sent from the worker to the coordinate actor
case class Register(execId: Int, url: String) extends Serializable
case class Heartbeat(id: String, responseTime: Long) extends Message

// sent from the client to the coordinate actor and forwarded by coordinate actor to the remote worker
case class QueryRequest(queryVector: SparseVector) extends Message

case class Insert(newVector: SparseVector) extends Message

// sent from the worker to the coordinator and forwarded to the client
case class QueryResponse(response: Array[SparseVector]) extends Message

// sent from coordinator to worker
case object IncreaseShard
