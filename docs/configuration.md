
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
</table>