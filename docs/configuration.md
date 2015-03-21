
<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>cpslab.lsh.kvEngineName</code></td>
  <td>"LevelDB"</td>
  <td>
	the name of the key value store engine saving LSH data. 
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.cacheEngineName</code></td>
  <td>(none)</td>
  <td>
  	the name of the cache engine. by default, there is no cache engine
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.deploy.clientHandlerInstanceNumber</code></td>
  <td>10</td>
  <td>
	The number of handlers per machine.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.name</code></td>
  <td>(none)</td>
  <td>
	The name of the lsh instance to create.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.familySize</code></td>
  <td>(none)</td>
  <td>
	the size of the hash family used in LSH schema.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.tableNum</code></td>
  <td>(none)</td>
  <td>
	The number of the hash tables used in LSH.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.vectorDim</code></td>
  <td>(none)</td>
  <td>
	The vector dimensionality.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.chainLength</code></td>
  <td>(none)</td>
  <td>
	The length of the hash functions chain used in each hash table.
  </td>  
</tr>

<tr>
  <td><code>cpslab.lsh.family.pstable.mu</code></td>
  <td>(none)</td>
  <td>
	mu value for gaussian distribution used in pstable family.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.family.pstable.sigma</code></td>
  <td>(none)</td>
  <td>
	sigma value for gaussian distribution used in pstable family.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.distributedSchema</code></td>
  <td>(none)</td>
  <td>
	The distributed schema of the system. Currently support schemas: PLSH, SHARDING. 
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.family.pstable.w</code></td>
  <td>(none)</td>
  <td>
	w value for gaussian distribution used in pstable family.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.similarityThreshold</code></td>
  <td>(none)</td>
  <td>
	the global threshold to select the most similar vectors.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.plsh.localActorNum</code></td>
  <td>(none)</td>
  <td>
	the number of the actors started in each node for PLSH schema.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.plsh.maxWorkerNum</code></td>
  <td>(none)</td>
  <td>
	The total number of worker started in PLSH schema.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.nodeID</code></td>
  <td>(none)</td>
  <td>
	localID of the node, used as the baseline of the worker ID.
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.generateMethod</code></td>
  <td>(none)</td>
  <td>
	defining the method on how to generate HashFamily; "default" -> create new HashFamily instance, 
	"fromfile" -> generate a hashchain from a fixed file
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.familyFilePath</code></td>
  <td>(none)</td>
  <td>
	the path of the file defining the hash family</td>
</tr>
<tr>
  <td><code>cpslab.lsh.sharding.maxShardNumPerTable</code></td>
  <td>(none)</td>
  <td>
	maximum number of shards *per table* allowed in the system
  </td>  
</tr>
<tr>
  <td><code>cpslab.lsh.sharding.maxShardDatabaseWorkerNum</code></td>
  <td>(none)</td>
  <td>
	maximum number of shard worker allowed in each process
  </td>
</tr>
<tr>
  <td><code>cpslab.lsh.sharding.namespace</code></td>
  <td>(none)</td>
  <td>
	sharding strategy of distributed LSH schema; 
	"independent" -> load balance with table first and then load balance with shardID
	"flat" -> load balance by making all buckets in all tables in a flat namespace; essentially it's balance with shard first and then on table
  </td>
</tr>
<tr>
  <td><code>cpslab.lsh.sharding.maxDatabaseNodeNum</code></td>
  <td>(none)</td>
  <td>
	Maximum number of nodes storing data for the table.
  </td>
</tr>
<tr>
  <td><code>cpslab.lsh.topK</code></td>
  <td>(none)</td>
  <td>
	select topK similar vectors.
  </td>
</tr>
<tr>
  <td><code>cpslab.lsh.writerActorNum</code></td>
  <td>(none)</td>
  <td>
  	The number of actors per Actor System receiving the intermediate results of similarity detection and send to clients.
  </td>
</tr>
</table>