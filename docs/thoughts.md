Questions:

1. **Q**: Why we not use a HBase/Cassandra/LevelDB as the key value store

   **A**: These databases are using LSM as the index system. The drawback of LSM is that the point queries performance are not satisfying. Though we can use Bloomfilter to help on this, it still fails on the range query case, because the successor of the key can be on any level of LSM.
      
      Besides that, in streaming LSH scenario, the key-value pairs are frequently updated (new item comes to the system). We need some way to efficiently support this expensive operation, that's upsert. Bloom filter does not help in this case, either. (about why we need range query, see 5 in design doc)
      
2. **Q**: if we stick to LSM, is there any problem preventing us from using HBase/Cassandra/LevelDB directly?

   **A**: (maybe we can use HBase as a baseline) 
   
   Update: 
   
   * If we use LevelDB, we need to implement the logic supporting query range across the cluster (Essentially a HMaster). 

   * If we use HBase/Cassandra, the current parallelism is not large enough to show the power of range query for mitigating hot point. (different architecture, because we cannot bind Region Server and the actor in the same node)
   

3. **Q**: where to use Fractal tree?

   **A**: each node maintaining the status 
   
   
4. **Q**: how to regulate the block size in each index node?

   **A**: HBase does not regulate that, b-tree (or b epsilon tree) based file system seems to regulate the block size by calling OS syscall.
   
   
5. **Q**: how to evaluate / theoriotically how to outperform LevelDB

   **A**: do not need to do that if we **use LevelDB as the storage backend**
   
6. How to implement load balancing mechanism where a lot of products go to some of the buckets of the table (load imbalance caused by the data skew)?
   
	  Possible Solutions:
       		   
      * Cuckoo Hashing (need to read MPLSH and NEST to see how to coordiate)
       
      * Modify the source code of Akka to support rich types of information as the evidence of imbalance
       
      * For those hot buckets, we split data in multiple nodes and maintain the data location in the entries (need rebalance)
      
      * In the leaf node level of the actor tree (actors maintaining the data in LSH table), apply the replication techniques, where we save data in multiple nodes and we apply a dynamic replication factor....(Scarlet), then use the power-of-two choices to select data (need rebalance)
      
      * change the index of the element by adding a random suffix, (very similar to how to mitigate HBase read/write pressure), and searching the similar items are transformed to a range query. (*) (need to implement a router which can transform a single range query request to multiple requests and send them to different actors)
      
         * We implement the routing strategy with ConsistentHashing logic and assume that we have known that what are the replication factors of each bucket
         
         * when the new request arrives, if it is related to the how buckets, we append the random key to the hash and rehash the key to decide where to save it (it's the written key), then we broadcast this request with replication factor replicas to send it to all nodes saving the message
         
         * adaptively adding factor number (not support decrease yet) 

  
 7. MP-LSH is a good solution for this case?
 
    yes, comparing to other papers, a single request is only generate 100 queries while others requires 700+
    
 8. what's the problem with LSH Forrest
 
    The query latency is too long (the tree height is 15)
    
    Using prefix-B-Tree as disk index (not good for writing)
    
    
9. How to deduplicate the results

    use a bitmap to deduplicate in server end first, and send the bitmap to the client, then duplicate in client and, in this way, we can minimize the network traffic amount (multiple shards maintained in the same server should also be deduplicated)
     
     
10. shall we put the similar elements (within the same or similar bucket index) in the same shards/machine?

    no, let's consider the following case

	We have 5 products and 1 m customers
	
	product 1 is very popular, 90% of the customers only bought this product and optionally bought another product
	
	so we will see that, a lot of vectors are just 1 bit off with the [1, 0, 0, 0, 0]..if we allocate similar elements in the same shard, we will see the hotspot
	
	So the wise way to do this is to distribute shards to multiple machines, and merge messages to reduce network cost.