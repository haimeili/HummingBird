package cpslab.db;

import cpslab.lsh.LocalitySensitiveHasher;
import scala.collection.mutable.StringBuilder;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ActorPartitionedHTreeBasic<K, V> extends PartitionedHTreeMap<K, V> {

  final int tableId;

  ConcurrentHashMap<String, StoreSegment> storageSpaces = new ConcurrentHashMap<>();

  protected HashMap<String, ReentrantReadWriteLock> structureLocks = new HashMap<>();

  protected final ConcurrentHashMap<String, ReentrantReadWriteLock> partitionRamLock =
          new ConcurrentHashMap<String, ReentrantReadWriteLock>();

  protected final ConcurrentHashMap<String, ReentrantReadWriteLock> partitionPersistLock =
          new ConcurrentHashMap<String, ReentrantReadWriteLock>();

  public ActorPartitionedHTreeBasic(
          int tableId, String hasherName, String workingDirectory,
          String name, Partitioner<K> partitioner,
          boolean closeEngine,
          int hashSalt,
          Serializer<K> keySerializer,
          Serializer<V> valueSerializer,
          Fun.Function1<V, K> valueCreator,
          ExecutorService executor,
          boolean closeExecutor,
          long ramThreshold) {
    super(tableId, hasherName, workingDirectory, name, partitioner, closeEngine, hashSalt,
            keySerializer, valueSerializer, valueCreator, executor, closeExecutor, ramThreshold);
    this.tableId = tableId;
  }

  protected String buildStorageName(int partitionId, int segId) {
    StringBuilder sb = new StringBuilder();
    sb.append("partition-" + partitionId + "-" + segId);
    return sb.toString();
  }

  private void initPartition(int partitionId, int segId) {
    //add root record for each partition
    String storageName = buildStorageName(partitionId, segId);
    StoreSegment storeSegment = new StoreSegment(
            storageName, Volume.UNSAFE_VOL_FACTORY, null, 32, 0, false, false,
            null, false, true, null);
    storeSegment.serializer = LN_SERIALIZER;
    storeSegment.init();
    if (storageSpaces.containsKey(storageName)) {
      storageSpaces.get(storageName).close();
    }
    if (storageSpaces.containsKey(storageName)) {
      System.out.println("FAULT: duplicate " + storageName);
    }
    storageSpaces.put(storageName, storeSegment);
    Long[] segIds = new Long[SEG];
    for (int i = 0; i < SEG; i++) {
      long partitionRoot = storageSpaces.get(storageName).put(new int[BITMAP_SIZE], DIR_SERIALIZER);
      //partitionRootRec.put(partitionId, partitionRoot);
      segIds[i] = partitionRoot;
    }
    partitionRootRec.put(partitionId, segIds);
    //initialize counterRecId
    Long [] counterRecIdArray = new Long[SEG];
    for (int i = 0; i < SEG; i++) {
      long counterRecId = storeSegment.put(0L, Serializer.LONG);
      counterRecIdArray[i] = counterRecId;
    }
    counterRecids.put(partitionId, counterRecIdArray);
  }

  protected String initPartitionIfNecessary(int partitionId, int segId) {
    String storageName = buildStorageName(partitionId, segId);
    Lock structureLock = null;
    try {
      structureLock = structureLocks.get(storageName).writeLock();
      structureLock.lock();
      if (!partitionRamLock.containsKey(storageName) ||
              !partitionPersistLock.containsKey(storageName)) {
        initPartition(partitionId, segId);
        ReentrantReadWriteLock ramLockArray = new ReentrantReadWriteLock();
        ReentrantReadWriteLock persistLockArray = new ReentrantReadWriteLock();
        partitionRamLock.put(storageName, ramLockArray);
        partitionPersistLock.put(storageName, persistLockArray);
      }
    } catch (Exception e){
      e.printStackTrace();
    } finally {
      if (structureLock == null) {
        System.out.println("cannot find lock for " + storageName);
      } else {
        structureLock.unlock();
      }
    }
    return storageName;
  }

  private PartitionedHTreeMap.LinkedNode<K, V> getInner(Object key, int seg, int h, int partition) {
    try {
      String storageName = buildStorageName(partition, seg);
      long recId = partitionRootRec.get(partition)[seg];
      Engine engine = storageSpaces.get(storageName);
      if (((Store) engine).getCurrSize() >= ramThreshold) {
        persist(partition);
      }
      return search(key, engine, recId, h);
    } catch (Exception npe) {
      npe.printStackTrace();
      return null;
    }
  }

  @Override
  public V get(final Object o) {
    if (o == null) return null;
    final int h = hash((K) o);
    final int seg;// = h >>> BUCKET_LENGTH;
    final int partition1 = partitioner.getPartition((K) o);
    int partition = 0;
    if (!(hasher instanceof LocalitySensitiveHasher)) {
      //if MainTable
      partition = Math.abs(partition1);
      seg = h % SEG;
    } else {
      partition = partition1;
      seg = h >>> BUCKET_LENGTH;
    }
    String storageName = buildStorageName(partition, seg);
    PartitionedHTreeMap.LinkedNode<K, V> ln;
    try {
      final Lock ramLock = partitionRamLock.get(storageName).readLock();
      try {
        ramLock.lock();
        ln = getInner(o, seg, h, partition);
      } finally {
        ramLock.unlock();
      }
    } catch (Exception npe) {
      npe.printStackTrace();
      System.out.println("fetch null at partition " + partition + ", at key " + o);
      return null;
    }

    if (valueCreator == null) {
      if (ln == null)
        return null;
      return ln.value;
    }

    //value creator is set, so create and put new value
    V value = valueCreator.run((K) o);
    //there is race condition, vc could be called twice. But map will be updated only once
    V prevVal = putIfAbsent((K) o, value);

    if (prevVal != null)
      return prevVal;
    return value;
  }

  @Override
  public V put(final K key, final V value) {
    if (key == null)
      throw new IllegalArgumentException("null key");

    if (value == null)
      throw new IllegalArgumentException("null value");

    V ret;
    final int h = hash(key);
    final int seg;// = h >>> BUCKET_LENGTH;
    final int partition = partitioner.getPartition(
            (K) (hasher instanceof LocalitySensitiveHasher ? h : key));
    if (!(hasher instanceof LocalitySensitiveHasher)) {
      seg = h % SEG;
    } else {
      seg = h >>> BUCKET_LENGTH;
    }
    initPartitionIfNecessary(partition, seg);
    try {
      partitionRamLock.get(buildStorageName(partition, seg)).writeLock().lock();
      ret = putInner(key, value, h, partition);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    } finally {
      partitionRamLock.get(buildStorageName(partition, seg)).writeLock().unlock();
    }
    return value;
  }

  public LinkedList<K> getSimilar(
          final Object key) {
    //TODO: Finish getSimilar
    final int h = hash((K) key);
    final int seg = h >>> BUCKET_LENGTH;
    final int partition = partitioner.getPartition(
            (K) (hasher instanceof LocalitySensitiveHasher ? h : key));

    LinkedList<K> lns;
    try {
      lns = getInnerWithSimilarity(key, seg, h, partition);
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      return null;
    }

    if (lns == null)
      return null;
    return lns;
  }

  protected LinkedList<K> getInnerWithSimilarity(
          final Object key,
          int seg,
          int h,
          int partition) {
    try {
      long recId = partitionRootRec.get(partition)[seg];
      Engine engine = storageSpaces.get(buildStorageName(partition, seg));
      if (((Store) engine).getCurrSize() >= ramThreshold) {
        persist(partition);
      }
      return searchWithSimilarity(key, engine, recId, h);
    } catch (NullPointerException npe) {
      npe.printStackTrace();
      return null;
    }
  }

  protected V storeVector(K key, V value, int h, long rootRecid, int partition, int seg,
                          Engine engine) {
    long dirRecid = rootRecid;
    int level = MAX_TREE_LEVEL;
    while (true) {
      Object dir = engine.get(dirRecid, DIR_SERIALIZER);
      //Nan: every NUM_BITS_PER_COMPARISON bits present the slot ID of the record
      final int slot = (h >>> (NUM_BITS_PER_COMPARISON * level)) & BITS_COMPARISON_MASK;

      if (CC.ASSERT && (slot > DIRECTORY_NODE_SIZE - 1))
        throw new DBException.DataCorruption("slot too high");
      if (dir == null) {
        //create new dir
        dir = new int[BITMAP_SIZE]; //Nan: 16 bytes, 128 bits
      }
      //Nan: dirOffset - the offset with in a dir
      final int dirOffset = dirOffsetFromSlot(dir, slot);
      int bucketConflictCost = 0;
      long recid = dirOffset < 0 ? 0 : dirGet(dir, dirOffset);
      if (recid != 0) {
        //Nan Zhu: the record id has existed
        if ((recid & 1) == 0) {
          //Nan Zhu: this is the directory node
          dirRecid = recid >>> 1;
          level--;
          continue;
        }
        recid = recid >>> 1;

        //traverse linked list, try to replace previous value
        PartitionedHTreeMap.LinkedNode<K, V> ln = engine.get(recid, LN_SERIALIZER);

        while (ln != null) {
          if (keySerializer.equals(ln.key, key)) {
            //found, replace value at this node
            V oldVal = ln.value;
            ln = new PartitionedHTreeMap.LinkedNode<K, V>(ln.next, ln.key, value);
            if (CC.ASSERT && ln.next == recid)
              throw new DBException.DataCorruption("cyclic reference in linked list");

            engine.update(recid, ln, LN_SERIALIZER);
            return oldVal;
          }
          recid = ln.next;
          ln = ((recid == 0) ? null : engine.get(recid, LN_SERIALIZER));
          if (CC.ASSERT && ln != null && ln.next == recid)
            throw new DBException.DataCorruption("cyclic reference in linked list");
          bucketConflictCost++;
          if (CC.ASSERT && bucketConflictCost > 1024 * 1024)
            throw new DBException.DataCorruption("linked list too large");
        }
        //key was not found at linked list, so just append it to beginning
      }

      //there is no such a null value
      //check if linked list has overflow and needs to be expanded to new dir level
      if (bucketConflictCost >= BUCKET_OVERFLOW && level >= 1) {
        Object newDirNode = new int[BITMAP_SIZE];
        {
          //Generate the new linkedNode
          final PartitionedHTreeMap.LinkedNode<K, V> node =
                  new PartitionedHTreeMap.LinkedNode<K, V>(0, key, value);
          //put the linkedNode to node and get the assigned record id
          final long newRecid = engine.put(node, LN_SERIALIZER);
          if (CC.ASSERT && newRecid == node.next)
            throw new DBException.DataCorruption("cyclic reference in linked list");
          //add newly inserted record
          //find the position of the node in the directory node in next level
          final int pos = (h >>> (NUM_BITS_PER_COMPARISON * (level - 1))) & BITS_COMPARISON_MASK;
          //update the dir node with the new LinkedNode
          newDirNode = putNewRecordIdInDir(newDirNode, pos, (newRecid << 1) | 1);
        }

        //redistribute linked bucket into new dir
        //Nan Zhu:
        //traverse all linked node under the same slot and put it in the new directory node
        //in the next level
        long nodeRecid = dirOffset < 0 ? 0 : dirGet(dir, dirOffset) >>> 1;
        while (nodeRecid != 0) {
          PartitionedHTreeMap.LinkedNode<K, V> n = engine.get(nodeRecid, LN_SERIALIZER);
          final long nextRecid = n.next;
          final int pos = (hash(n.key) >>> (NUM_BITS_PER_COMPARISON * (level - 1))) &
                  BITS_COMPARISON_MASK;
          final long recid2 = dirGetSlot(newDirNode, pos);
          n = new PartitionedHTreeMap.LinkedNode<K, V>(recid2 >>> 1, n.key, n.value);
          //Nan Zhu: put in the new record node
          newDirNode = putNewRecordIdInDir(newDirNode, pos, (nodeRecid << 1) | 1);
          engine.update(nodeRecid, n, LN_SERIALIZER);
          if (CC.ASSERT && nodeRecid == n.next)
            throw new DBException.DataCorruption("cyclic reference in linked list");
          nodeRecid = nextRecid;
        }

        //insert nextDir and update parent dir
        long nextDirRecid = engine.put(newDirNode, DIR_SERIALIZER);
        int parentPos = (h >>> (NUM_BITS_PER_COMPARISON * level)) & BITS_COMPARISON_MASK;
        //update the parent directory node
        dir = putNewRecordIdInDir(dir, parentPos, (nextDirRecid << 1) | 0);
        engine.update(dirRecid, dir, DIR_SERIALIZER);
        //update counter
        // counter(partition, seg, engine, +1);

        return null;
      } else {
        //Nan Zhu:
        // record does not exist in linked list and the linked list hasn't overflow,
        // so create new one
        recid = dirOffset < 0 ? 0 : dirGet(dir, dirOffset) >>> 1;
        //Nan: insert at the head of the linked list
        //the recid/2 === the first record under this slot
        final long newRecid = engine.put(
                new PartitionedHTreeMap.LinkedNode<K, V>(recid, key, value),
                LN_SERIALIZER);
        if (CC.ASSERT && newRecid == recid) {
          throw new DBException.DataCorruption("cyclic reference in linked list");
        }
        dir = putNewRecordIdInDir(dir, slot, (newRecid << 1) | 1);
        engine.update(dirRecid, dir, DIR_SERIALIZER);
        //update counter
        // counter(partition, seg, engine, +1);
        return null;
      }
    }
  }

  @Override
  protected V putInner(K key, V value, int h, int partition) {
    int seg;
    if (!(hasher instanceof LocalitySensitiveHasher)) {
      seg = h % SEG;
    } else {
      seg = h >>> BUCKET_LENGTH;
    }
    long dirRecid = partitionRootRec.get(partition)[seg];
    String storageName = buildStorageName(partition, seg);
    Engine engine = storageSpaces.get(storageName);
    if (engine == null) {
      System.out.println("FAULT: cannot find engine for " + storageName);
      System.exit(1);
    }
    return storeVector(key, value, h, dirRecid, partition, seg, engine);
  }

  @Override
  public void initStructureLocks() {
    int partitionNum = partitioner.numPartitions;
    int segNum = PartitionedHTreeMap.SEG;
    for (int i = 0; i < partitionNum; i++) {
      for (int j = 0; j < segNum; j++) {
        structureLocks.put(buildStorageName(i, j), new ReentrantReadWriteLock());
      }
    }
  }
}
