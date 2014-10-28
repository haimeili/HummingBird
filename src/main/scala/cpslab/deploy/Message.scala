package cpslab.deploy

import org.apache.spark.mllib.linalg.SparseVector

sealed trait Message extends Serializable

case class Query(queryVector: SparseVector) extends Message

case class Insert(newVector: SparseVector) extends Message
