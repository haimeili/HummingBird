##Distributed LSH design 

### Drawbacks of PLSH ###

1. The system is parallelized based on data partition, so that the single query will have to be routed to all nodes in the system to get all similar candidates.

2. The system does not provide any fault-tolerance mechanism, so if one of the nodes is crashed, no way to recover the data and get accurate result any more

3. The system can only utilize the in-memory space, limiting the overall system capacity 

4. Related to 3, the system requires to retire old data and limiting the servers serving write operation within a certain group of servers.


### Solutions ###

1. New Distributed Paradigm 

   1.1 try to use a more efficient LSH scheme (Multi-probe LSH?) to avoid large scale broadcast message
   
   1.2 use cluster sharding technology to split data based on the hashing bucket index 
   
     * Sharding each hash table. //TODO: considering the data sharding strategy
   
     * For each hash table, we elect a node as the proxy node, which is a cluster-aware router serving specific role. ***This router is for handling client request and locates on every machine serving the table data (for fault-tolerance)***. The proxy node broadcasts the client requests to each ShardBoard.
       
     * The ShardBoard is essentially the ShardRegion, which manages the shards locating in the node. 
       
     * ShardRegion forwards the messages to the entries according to hashing bucket index, which are essentially the Pool routers. (should implement a dynamic approach to support elastic resource allocation) 
                   
     * The entries (this is where the load balancing in the 6 of thoughts document locates) starts children to maintain the data saved in hash table.  (** note: implementing the entries as Pool router makes it hard/impossible to extend to get the functionality in the third entry of 6 in "thoughts" document, unless that we implement the entry as the customized router or we sacrifice the performance by implemeting entries as simple actors  **)
   
         
2. fault-tolerance

   2.1 persist data in the disk with journal or snapshot 

   2.2 Migration of actors  

3. data structure to manage disk and in-memory space

4. data structure offering high write-throughput (Write Optimized Index) (to avoid the random write when the streaming data is irregular)

	can use prefetching techniques to avoid point queries...
	
5. use an efficient bitmap structure to deduplicate the results

   

6. Shard and Entry ID Resolver Design 

   ShardID = Hash(request) + RandomKey
   
   EntryID = ShardID % MaxEntryNum
   
7. Algorithm of mitigating hot spot:

   Basic Version:
   
   Step 1: Maintain a data structure recording the shards which have the multiple allocations

   Step 2: Hash the request to get the first part of shardID 
   
   Step 3: if the ShardID is having multiple allocations, attach a RandomKey to the ShardID got in Step 2
   
   Step 4: Forward this request to other machines through ShardLord

   Problem 1: how to decide which index should increase replication factor	   
   In each server, the client handler samples the access pattern of the client request (k, maintains top k most frequently accessed shard) and report to the singleton actor node. (smooth sliding to avoid bursty load and a max-heap to ensure that only maintaining the minimum number of shards) (TODO: revise)
   
   The singleton node decides which shard(s) should be amortized to multiple nodes (t, the skew threshold threhold).
   
   Step 1, move node data to the target node
   
   Step 2, Two-phase commit protocol to ensure that, all clients agree to change the point query to the particular shard to a range query (timer based)
   
   
   
   









