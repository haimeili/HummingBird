package cpslab.deploy

import scala.collection.immutable.HashMap
import scala.collection.mutable

import akka.actor.ActorRef
import akka.contrib.pattern.ShardRegion.ShardId
import cpslab.lsh.vector.{SparseVector, SparseVectorWrapper}
import cpslab.storage.LongBitSet

sealed trait SimilaritySearchMessages extends Serializable

sealed trait ShardAllocation extends SimilaritySearchMessages

// messages for the basic communication between nodes (client-server, server-server)
/**
 * messages sent from client to server, representing the request for similarity search
 * @param vector the vector data
 */
case class SearchRequest(vector: SparseVector) extends SimilaritySearchMessages

/**
 * the result of the similarity search
 * @param queryVectorID the unique ID representing the query vector
 * @param bitmap the bitmap representing similarVectors             
 * @param similarVectorPairs the ID of the similar vectors
 * @param latency the optional latency parameter indicating the time cost to get this output
 */
case class SimilarityOutput(
    queryVectorID: Int, bitmap: LongBitSet,
    similarVectorPairs: List[(Int, Double)],
    latency: Option[Long] = None)

// messages for the communication between nodes in the cluster sharding schema

/**
 * this message represents the allocation of the shards of the vectors
 * the message can be sent from the shardRegions to the entry actors (EntryResolver) and also can be
 * sent from the actors calculating the allocated shards to the local shardRegions
 *
 * NOTE: to correctly perform the funcitonality, we need to ensure that all shardids contained in
 * this class belongs to the same ShardRegion
 *
 * @param shardsMap (ShardID -> (vector, tableIDs)
 */
case class FlatShardAllocation(shardsMap: HashMap[ShardId,
  mutable.HashMap[SparseVectorWrapper, Array[Int]]]) extends ShardAllocation

/**
 * message triggering the IO operation in ShardDatabaseWorker
 */
case object IOTicket
