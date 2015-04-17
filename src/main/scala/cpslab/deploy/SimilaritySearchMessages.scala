package cpslab.deploy

import scala.collection.mutable

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
 * the intermediate result of the similarity search
 * @param queryVectorID the unique ID representing the query vector
 * @param bitmap the bitmap representing the similarVectorPairs
 * @param similarVectorPairs the ID of the similar vectors as well as the corresponding similarity 
 *                           value; NOTE: though in server end, we can use bitmap to de-duplicate 
 *                           and reduce the network traffic amount, we rely on the client-end 
 *                           further deduplicate to select the final topK
 */
case class SimilarityIntermediateOutput(
    queryVectorID: Int,
    bitmap: LongBitSet,
    similarVectorPairs: List[(Int, Double)]) extends SimilaritySearchMessages

/**
 * the result of the similarity search
 * @param queryID the unique ID representing the query vector
 * @param bitmap the bitmap representing similarVectors             
 * @param similarVectors the ID of the similar vectors
 * @param latency the optional latency parameter indicating the time cost to get this output
 */
case class SimilarityOutput(queryID: Int, bitmap: LongBitSet, similarVectors: List[(Int, Double)],
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
 * @param shardsMap (TableID -> (ShardID, vectors))
 */
case class PerTableShardAllocation(shardsMap: mutable.HashMap[Int,
    mutable.HashMap[ShardId, List[SparseVectorWrapper]]]) extends ShardAllocation


/**
 * this message represents the allocation of the shards of the vectors
 * the message can be sent from the shardRegions to the entry actors (EntryResolver) and also can be
 * sent from the actors calculating the allocated shards to the local shardRegions
 *
 * NOTE: to correctly perform the funcitonality, we need to ensure that all shardids contained in
 * this class belongs to the same ShardRegion
 *
 * @param shardsMap (ShardID -> (TableID, vectors)
 */
case class FlatShardAllocation(shardsMap: mutable.HashMap[ShardId,
  mutable.HashMap[Int, List[SparseVectorWrapper]]]) extends ShardAllocation

/**
 * the class representing the request to index sparseVectors in certain table
 * this request also serves as the query request
 * NOTE: we need to ensure that, all sparse vectors represented in this class belongs to the same 
 * entry
 * @param indexMap shardID(independent)/tableID(flat) -> vectors
 */
case class LSHTableIndexRequest(indexMap: mutable.HashMap[Int, List[SparseVectorWrapper]])
  extends SimilaritySearchMessages

/**
 * message triggering the IO operation in ShardDatabaseWorker
 */
case object IOTicket
