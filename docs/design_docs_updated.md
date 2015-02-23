##Distributed LSH design 

### Drawbacks of PLSH ###

1. The system is parallelized based on data partition, so that the single query will have to be routed to all nodes in the system to get all similar candidates.

2. The system does not provide any fault-tolerance mechanism, so if one of the nodes is crashed, no way to recover the data and get accurate result any more

3. The system can only utilize the in-memory space, limiting the overall system capacity 

### Solutions ###

1. New Distributed Paradigm 

   1.1 try to use a more efficient LSH scheme (Multi-probe LSH?) to avoid large scale broadcast message
   
   1.2 use cluster sharding technology to split data based on the hashing bucket index (re-use the LSH functionality result to avoid calculating lsh value for each time) 
   
     * for each hash table, we elect a node as the proxy node, the cluster-aware router broadcast the node to each proxy node.
       
     * The proxy node is essentially the shard lord, which manages the shards locating in the node. 
       
     * Proxy node forwards the messages to the entries according to hashing bucket index, which are essentially the Pool routers. (should implement a dynamic approach to support elastic resource allocation) 
                   
     * The entries starts children to maintain the data saved in hash table. (this is where the load balancing in 1.3 locates) (** note: implementing the entries as Pool structures makes it hard/impossible to extend to get the functionality in the third entry of 1.3, except we implement the entry as the customized router or we sacrifice the performance by implemeting entries as simple actors  **)
   
   1.3 How to implement load balancing mechanism where a lot of products go to some of the buckets of the table (load imbalance caused by the data skew)?
   
	  Possible Solutions:
       		   
      * Cuckoo Hashing (need to read MPLSH and NEST to see how to coordiate)
       
      * Modify the source code of Akka to support rich types of information as the evidence of imbalance
       
      * For those hot buckets, we split data in multiple nodes and maintain the data location in the entries (need rebalance)
      
      * In the leaf node level of the actor tree (actors maintaining the data in LSH table), apply the replication techniques, where we save data in multiple nodes and we apply a dynamic replication factor....(Scarlet), then use the power-of-two choices to select data (need rebalance)
      
2. fault-tolerance

   2.1 persist data in the disk with journal or snapshot 

   2.2 Migration of actors  

3. data structure to manage disk and in-memory space












