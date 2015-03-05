package cpslab.deploy

import akka.contrib.pattern.ShardRegion.ShardId
import cpslab.vector.SparseVector

sealed trait SimilaritySearchMessages extends Serializable

// messages for the basic communication between nodes (client-server, server-server)
/**
 * messages sent from client to server, representing the request for similarity search
 * @param vectorId the unique ID representing the query vector
 * @param vector the vector data
 * @param topK select the most k similar vectors, default to be 0, then we rely on the global 
 *             similarity threshold (cpslab.lsh.similarityThreshold) to select the most similar 
 *             vectors
 *                          
 */
case class SearchRequest(vectorId: String, vector: SparseVector, topK: Int = 0)
  extends SimilaritySearchMessages

/**
 * the result of the similarity search
 * @param queryVectorID the unique ID representing the query vector 
 * @param similarVectorPairs the ID of the similar vectors as well as the corresponding similarity 
 *                           value; NOTE: though in server end, we can use bitmap to de-duplicate 
 *                           and reduce the network traffic amount, we rely on the client-end 
 *                           further deduplicate to select the final topK
 */
case class SimilarityOutput(queryVectorID: String, similarVectorPairs: (String, Double) *) 
  extends SimilaritySearchMessages

// messages for the communication between nodes in the cluster sharding schema

/**
 * this message represents the allocation of the shards of the vectors
 * the message can be sent from the shardRegions to the entry actors (EntryResolver) and also can be
 * sent from the actors calculating the allocated shards to the other shardRegions
 * @param shardIDs the IDs of the shards
 * @param vectors the list of the vector which would be allocated to the shards with the IDs in the 
 *                prior shardIDs list
 */
case class ShardAllocation(shardIDs: List[ShardId], vectors: List[SparseVector])

