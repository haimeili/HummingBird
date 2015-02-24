Questions:

1. **Q**: Why we not use a HBase/Cassandra/LevelDB as the key value store

   **A**: These databases are using LSM as the index system. The drawback of LSM is that the point queries performance are not satisfactorying. Though we can use Bloomfilter to help on this, it still fails on the range query case, because the successor of the key can be on any level of LSM.
      
      Besides that, in streaming LSH scenario, the key-value pairs are frequently updated (new item comes to the system). We need some way to efficiently support this expensive operation, that's upsert. Bloom filter does not help in this case, either. (about why we need range query, see 5 in design doc)
      
2. **Q**: if we stick to LSM, is there any problem preventing us from using HBase/Cassandra/LevelDB directly?

   **A**: No (maybe we can use HBase as a baseline)
   
 
     