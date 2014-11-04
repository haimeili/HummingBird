package cpslab.sharding

import akka.actor.ActorRef
import akka.contrib.pattern.ShardCoordinator.ShardAllocationStrategy
import akka.contrib.pattern.ShardRegion.ShardId

import scala.collection.immutable.IndexedSeq

class LatencyAwareShardingStrategy extends ShardAllocationStrategy{
  override def allocateShard(requester: ActorRef, shardId: ShardId,
                             currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]]): ActorRef = {

    //TODO: maybe implement the power of two choice?
    null
  }

  override def rebalance(currentShardAllocations: Map[ActorRef, IndexedSeq[ShardId]],
                         rebalanceInProgress: Set[ShardId]): Set[ShardId] = {
    //TODO: possible solution, test the latency(RTT) of the region with the most shards
    //(first x percentile?)
    null
  }
}
