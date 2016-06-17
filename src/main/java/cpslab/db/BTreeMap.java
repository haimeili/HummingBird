/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * NOTE: some code (and javadoc) used in this class
 * comes from JSR-166 group with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package cpslab.db;


import cpslab.deploy.BTreeDatabase;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.ReentrantLock;


/**
 * <p>
 * A scalable concurrent {@link ConcurrentNavigableMap} implementation.
 * The map is sorted according to the {@linkplain Comparable natural
 * ordering} of its keys, or by a {@link Comparator} provided at map
 * creation time.
 * </p><p>
 * <p/>
 * Insertion, removal,
 * update, and access operations safely execute concurrently by
 * multiple threads.  Iterators are <i>weakly consistent</i>, returning
 * elements reflecting the state of the map at some point at or since
 * the creation of the iterator.  They do <em>not</em> throw {@link
 * ConcurrentModificationException}, and may proceed concurrently with
 * other operations. Ascending key ordered views and their iterators
 * are faster than descending ones.
 * </p><p>
 * <p/>
 * It is possible to obtain <i>consistent</i> iterator by using <code>snapshot()</code>
 * method.
 * </p><p>
 * <p/>
 * All <tt>Map.Entry</tt> pairs returned by methods in this class
 * and its views represent snapshots of mappings at the time they were
 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
 * method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
 * <tt>replace</tt>, depending on exactly which effect you need.)
 * </p><p>
 * <p/>
 * This collection has optional size counter. If this is enabled Map size is
 * kept in {@link Atomic.Long} variable. Keeping counter brings considerable
 * overhead on inserts and removals.
 * If the size counter is not enabled the <tt>size</tt> method is <em>not</em> a constant-time operation.
 * Determining the current number of elements requires a traversal of the elements.
 * </p><p>
 * <p/>
 * Additionally, the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
 * atomically. For example, an iterator operating concurrently with a
 * <tt>putAll</tt> operation might view only some of the added
 * elements. NOTE: there is an optional
 * </p><p>
 * <p/>
 * This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces. Like most other concurrent collections, this class does
 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
 * null return values cannot be reliably distinguished from the absence of
 * elements.
 * </p><p>
 * <p/>
 * Theoretical design of BTreeMap is based on 1986 paper
 * <a href="http://www.sciencedirect.com/science/article/pii/0022000086900218">
 * Concurrent operations on B∗-trees with overtaking</a>
 * written by Yehoshua Sagiv.
 * More practical aspects of BTreeMap implementation are based on
 * <a href="http://cs.au.dk/~tyoung/~td202/">notes</a>
 * and <a href="http://cs.au.dk/~tyoung/~td202/btree/">demo application</a> from Thomas Dinsdale-Young.
 * Also more work from Thomas: <a href="http://www.doc.ic.ac.uk/research/technicalreports/2011/#10">A Simple Abstraction for Complex Concurrent Indexes</a>
 * </p><p>
 * <p/>
 * B-Linked-Tree used here does not require locking for read.
 * Updates and inserts locks only one, two or three nodes.
 * Original BTree design does not use overlapping lock (lock is released before parent node is locked), I added it just to feel safer.
 * </p><p>
 * <p/>
 * This B-Linked-Tree structure does not support removal well, entry deletion does not collapse tree nodes. Massive
 * deletion causes empty nodes and performance lost. There is workaround in form of compaction process, but it is not
 * implemented yet.
 * </p>
 *
 * @author Jan Kotek
 * @author some parts by Doug Lea and JSR-166 group
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class BTreeMap<K, V>
        extends AbstractMap<K, V>
        implements ConcurrentNavigableMap<K, V>,
        Bind.MapWithModificationListener<K, V>,
        Closeable {

  /**
   * recid under which reference to rootRecid is stored
   */
  protected final long rootRecidRef;

  /**
   * Serializer used to convert keys from/into binary form.
   */
  protected final BTreeKeySerializer keySerializer;

  /**
   * Serializer used to convert values from/into binary form
   */
  protected final Serializer<V> valueSerializer;

  /**
   * holds node level locks
   */
  protected final LongConcurrentHashMap<ReentrantLock> nodeLocks = new LongConcurrentHashMap<>();

  /**
   * maximal node size allowed in this BTree
   */
  protected final int maxNodeSize;

  /**
   * DB Engine in which entries are persisted
   */
  protected final Engine engine;

  /**
   * is this a Map or Set?  if false, entries do not have values, only keys are allowed
   */
  protected final boolean hasValues;

  /**
   * store values as part of BTree nodes
   */
  protected final boolean valsOutsideNodes;

  protected final List<Long> leftEdges;


  private final KeySet keySet;

  private final EntrySet entrySet;

  private final Values values = new Values(this);

  private final ConcurrentNavigableMap<K, V> descendingMap = new DescendingMap(this, null, true, null, false);

  protected final Atomic.Long counter;

  protected final int numberOfNodeMetas;
  /**
   * Indicates if this collection collection was not made by DB by user.
   * If user can not access DB object, we must shutdown Executor and close Engine ourself in close() method.
   */
  protected final boolean closeEngine;

  public int tableId;


  /**
   * hack used for DB Catalog
   */
  protected static SortedMap<String, Object> preinitCatalog(DB db) {

    Long rootRef = db.getEngine().get(Engine.RECID_NAME_CATALOG, Serializer.RECID);

    BTreeKeySerializer keyser = BTreeKeySerializer.STRING;
    //$DELAY$
    if (rootRef == null) {
      if (db.getEngine().isReadOnly())
        return Collections.unmodifiableSortedMap(new TreeMap<String, Object>());

      NodeSerializer rootSerializer = new NodeSerializer(false, BTreeKeySerializer.STRING,
              db.getDefaultSerializer(), 0);
      BNode root = new LeafNode(keyser.emptyKeys(), true, true, false, new Object[]{}, 0);
      rootRef = db.getEngine().put(root, rootSerializer);
      //$DELAY$
      db.getEngine().update(Engine.RECID_NAME_CATALOG, rootRef, Serializer.RECID);
      db.getEngine().commit();
    }
    Serializer valser = db.getDefaultSerializer();
    if (CC.ASSERT && valser == null)
      throw new AssertionError();
    return new BTreeMap<String, Object>(
            db.engine,
            false,
            Engine.RECID_NAME_CATALOG,
            32,
            false,
            0,
            keyser,
            valser,
            0
    );
  }


  /**
   * if <code>valsOutsideNodes</code> is true, this class is used instead of values.
   * It contains reference to actual value. It also supports assertions from preventing it
   * to leak outside of Map
   */
  protected static final class ValRef {

    int currentLevel = 0;
    /**
     * reference to actual value
     */
    final List<Long> recids;

    public ValRef(List<Long> recid) {
      this.recids = recid;
    }

    public void appendNewRecId(long recId) {
      if (recids.contains(recId)) {
        System.out.println("FAULT: record " + recId + " already exists, thread: " +
                Thread.currentThread().getName());
        System.exit(1);
      }
      recids.add(recId);
    }

    @Override
    public boolean equals(Object obj) {
      throw new IllegalAccessError();
    }

    @Override
    public int hashCode() {
      throw new IllegalAccessError();
    }

    @Override
    public String toString() {
      return "BTreeMap-ValRef[" + recids.toString() + "][Level "+ currentLevel + "]";
    }
  }

  protected static final class ValRefSerializer extends Serializer<ValRef> {

    @Override
    public void serialize(DataOutput out, ValRef value) throws IOException {
      Serializer.INTEGER.serialize(out, value.currentLevel);
      Serializer.BASIC.serialize(out, value.recids);
    }

    @Override
    public ValRef deserialize(DataInput in, int available) throws IOException {
      int currentLevel = Serializer.INTEGER.deserialize(in, 4);
      ValRef v = new ValRef((List<Long>) Serializer.BASIC.deserialize(in, -1));
      v.currentLevel = currentLevel;
      return v;
    }

    @Override
    public boolean isTrusted() {
      return true;
    }

    @Override
    public boolean equals(ValRef a1, ValRef a2) {
      throw new IllegalAccessError();
    }

    @Override
    public int hashCode(ValRef valRef) {
      throw new IllegalAccessError();
    }
  }

  /**
   * common interface for BTree node
   */
  public abstract static class BNode {

    static final int LEFT_MASK = 1;
    static final int RIGHT_MASK = 1 << 1;
    static final int TOO_LARGE_MASK = 1 << 2;

    final Object keys;
    final byte flags;


    public BNode(Object keys, boolean leftEdge, boolean rightEdge, boolean tooLarge) {
      this.keys = keys;

      this.flags = (byte) (
              (leftEdge ? LEFT_MASK : 0) |
                      (rightEdge ? RIGHT_MASK : 0) |
                      (tooLarge ? TOO_LARGE_MASK : 0)
      );
    }


    final public Object key(BTreeKeySerializer keyser, int pos) {
      if (isLeftEdge()) {
        if (pos-- == 0)
          return null;
      }

      if (pos == keyser.length(keys) && isRightEdge())
        return null;
      return keyser.getKey(keys, pos);
    }

    final public int keysLen(BTreeKeySerializer keyser) {
      return keyser.length(keys) + leftEdgeInc() + rightEdgeInc();
    }

    final public boolean isLeftEdge() {
      return (flags & LEFT_MASK) != 0;
    }


    final public boolean isRightEdge() {
      return (flags & RIGHT_MASK) != 0;
    }


    /**
     * @return 1 if is left edge, or 0
     */
    final public int leftEdgeInc() {
      return flags & LEFT_MASK;
    }


    /**
     * @return 1 if is right edge, or 0
     */
    final public int rightEdgeInc() {
      return (flags & RIGHT_MASK) >>> 1;
    }


    final public boolean isTooLarge() {
      return (flags & TOO_LARGE_MASK) != 0;
    }

    public abstract boolean isLeaf();

    public abstract Object val(int pos, Serializer valueSerializer);

    final public Object highKey(BTreeKeySerializer keyser) {
      if (isRightEdge())
        return null;
      return keyser.getKey(keys, keyser.length(keys) - 1);
    }


    public abstract Object childArray();

    public abstract long child(int i);

    public abstract long next();

    public final int compare(final BTreeKeySerializer keyser, int pos1, int pos2) {
      if (pos1 == pos2)
        return 0;
      //$DELAY$
      if (isLeftEdge()) {
        //first position is negative infinity, so everything else is bigger
        //first keys is missing in array, so adjust positions
        if (pos1-- == 0)
          return -1;
        if (pos2-- == 0)
          return 1;
      }
      //$DELAY$
      if (isRightEdge()) {
        int keysLen = keyser.length(keys);
        //last position is positive infinity, so everything else is smaller
        if (pos1 == keysLen)
          return 1;
        if (pos2 == keysLen)
          return -1;
      }

      return keyser.compare(keys, pos1, pos2);
    }

    public final int compare(final BTreeKeySerializer keyser, int pos, Object second) {
      if (isLeftEdge()) {
        //first position is negative infinity, so everything else is bigger
        //first keys is missing in array, so adjust positions
        if (pos-- == 0)
          return -1;
      }
      //$DELAY$
      if (isRightEdge() && pos == keyser.length(keys)) {
        //last position is positive infinity, so everything else is smaller
        return 1;
      }
      return keyser.compare(keys, pos, second);
    }


    public void checkStructure(BTreeKeySerializer keyser, Serializer valser) {
      //check all keys are sorted;
      if (keyser == null)
        return;

      int keylen = keyser.length(keys);
      int end = keylen - 2 + rightEdgeInc();
      if (end > 1) {
        for (int i = 1; i <= end; i++) {
          if (keyser.compare(keys, i - 1, i) >= 0)
            throw new DBException.DataCorruption("keys are not sorted: " + Arrays.toString(keyser.keysToArray(keys)));
        }
      }
      //check last key is sorted or null
      if (!isRightEdge() && keylen > 2) {
        if (keyser.compare(keys, keylen - 2, keylen - 1) > 0) {
          throw new DBException.DataCorruption("Last key is not sorted: " + Arrays.toString(keyser.keysToArray(keys)));
        }
      }
    }

    public abstract BNode copyAddKey(BTreeKeySerializer keyser, Serializer valser, int pos, Object newKey, long newChild, Object newValue);

    public abstract BNode copySplitRight(BTreeKeySerializer keyser, Serializer valser, int splitPos);

    public abstract BNode copySplitLeft(BTreeKeySerializer keyser, Serializer valser, int splitPos, long newNext);

    public abstract int valSize(Serializer valueSerializer);

    public abstract int childArrayLength();
  }

  public final static class DirNode extends BNode {
    final Object child;

    DirNode(Object keys, boolean leftEdge, boolean rightEdge, boolean tooLarge, Object child) {
      super(keys, leftEdge, rightEdge, tooLarge);
      this.child = child;

      if (CC.ASSERT)
        checkStructure(null, null);
    }


    @Override
    public boolean isLeaf() {
      return false;
    }

    @Override
    public Object val(int pos, Serializer valueSerializer) {
      return null;
    }

    @Override
    public Object childArray() {
      return child;
    }

    @Override
    public long child(int pos) {
      Object c = child;
      return c instanceof int[] ?
              ((int[]) c)[pos] :
              ((long[]) c)[pos];
    }

    @Override
    public int childArrayLength() {
      return child instanceof int[] ?
              ((int[]) child).length :
              ((long[]) child).length;
    }


    @Override
    public long next() {
      Object c = child;
      if (c instanceof int[]) {
        int[] cc = (int[]) c;
        return cc[cc.length - 1];
      } else {
        long[] cc = (long[]) c;
        return cc[cc.length - 1];
      }
    }

    @Override
    public String toString() {
      return "Dir(" + leftEdgeInc() + "-" + rightEdgeInc() + "-K" + Fun.toString(keys) + ", C" + Fun.toString(child) + ")";
    }


    @Override
    public void checkStructure(BTreeKeySerializer keyser, Serializer valser) {
      super.checkStructure(keyser, valser);

      int childLen = child instanceof int[] ? ((int[]) child).length : ((long[]) child).length;

      if (keyser != null && childLen != keysLen(keyser))
        throw new DBException.DataCorruption("bnode has inconsistent lengths");

      if ((isRightEdge() != (next() == 0)))
        throw new DBException.DataCorruption("bnode right edge inconsistent with link");

    }

    @Override
    public DirNode copyAddKey(BTreeKeySerializer keyser, Serializer valser, int pos, Object newKey,
                              long newChild, Object newValue) {
      Object keys2 = keyser.putKey(keys, pos - leftEdgeInc(), newKey);

      Object child2;
      //expand children and put new child
      if (child instanceof int[]) {
        int[] child_ = (int[]) child;
        if (newChild < Integer.MAX_VALUE) {
          //operate on ints
          final int[] child2_ = Arrays.copyOf(child_, child_.length + 1);
          child2 = child2_;
          if (pos < child_.length) {
            System.arraycopy(child_, pos, child2_, pos + 1, child_.length - pos);
          }
          child2_[pos] = (int) newChild;

        } else {
          //new child does not fit, must upgrade to long[]
          long[] child2_ = new long[child_.length + 1];
          child2 = child2_;
          for (int i = 0; i < pos; i++) {
            child2_[i] = child_[i];
          }
          child2_[pos] = newChild;
          for (int i = pos + 1; i < child2_.length; i++) {
            child2_[i] = child_[pos - 1];
          }
        }
      } else {
        long[] child_ = (long[]) child;
        //operate on ints
        final long[] child2_ = Arrays.copyOf(child_, child_.length + 1);
        child2 = child2_;
        if (pos < child_.length) {
          System.arraycopy(child_, pos, child2_, pos + 1, child_.length - pos);
        }
        child2_[pos] = (int) newChild;
      }

      //$DELAY$
      return new DirNode(keys2, isLeftEdge(), isRightEdge(), false, child2);
    }

    @Override
    public DirNode copySplitRight(BTreeKeySerializer keyser, Serializer valser, int splitPos) {
      int keylen = keyser.length(keys);
      Object keys2 = keyser.copyOfRange(keys, splitPos - leftEdgeInc(), keylen);
      //$DELAY$
      Object child2;
      if (child instanceof int[]) {
        int[] child_ = (int[]) child;
        child2 = Arrays.copyOfRange(child_, splitPos, child_.length);
      } else {
        long[] child_ = (long[]) child;
        child2 = Arrays.copyOfRange(child_, splitPos, child_.length);
      }
      //$DELAY$
      return new DirNode(keys2, false, isRightEdge(), false, child2);
    }

    @Override
    public DirNode copySplitLeft(BTreeKeySerializer keyser, Serializer valser, int splitPos, long newNext) {
      Object keys2 = keyser.copyOfRange(keys, 0, splitPos + 1 - leftEdgeInc());

      Object child2;
      boolean intArrayInstance = child instanceof int[];
      if (intArrayInstance) {
        int[] child_ = (int[]) child;
        if (newNext < Integer.MAX_VALUE) {
          //all fits operate on int[]
          int[] child2_ = Arrays.copyOf(child_, splitPos + 1);
          child2_[splitPos] = (int) newNext;
          child2 = child2_;
        } else {
          //does not fit, upgrade to long
          long[] child2_ = new long[splitPos + 1];
          for (int i = 0; i <= splitPos; i++) {
            child2_[i] = child_[i];
          }
          child2_[splitPos] = newNext;
          child2 = child2_;
        }
      } else {
        long[] child2_ = Arrays.copyOf((long[]) child, splitPos + 1);
        child2_[splitPos] = (int) newNext;
        child2 = child2_;
      }

      return new DirNode(keys2, isLeftEdge(), false, false, child2);
    }

    @Override
    public int valSize(Serializer valueSerializer) {
      throw new UnsupportedOperationException("dirnode");
    }


  }


  public final static class LeafNode extends BNode {
    final Object vals;
    final long next;

    LeafNode(Object keys, boolean leftEdge, boolean rightEdge, boolean tooLarge, Object vals, long next) {
      super(keys, leftEdge, rightEdge, tooLarge);
      this.vals = vals;
      this.next = next;

      if (CC.ASSERT)
        checkStructure(null, null);
    }

    @Override
    public boolean isLeaf() {
      return true;
    }

    @Override
    public Object val(int pos, Serializer valueSerializer) {
      return valueSerializer.valueArrayGet(vals, pos);
    }

    @Override
    public byte[] childArray() {
      return null;
    }

    @Override
    public long child(int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long next() {
      return next;
    }

    @Override
    public String toString() {
      String valsStr = Fun.toString(vals); //TODO use value serializer to turn this into string

      return "Leaf(" + leftEdgeInc() + "-" + rightEdgeInc() + "-" + "K" + Fun.toString(keys) + ", V" + valsStr + ", L=" + next + ")";
    }

    @Override
    public void checkStructure(BTreeKeySerializer keyser, Serializer valser) {
      super.checkStructure(keyser, valser);
      if ((next == 0) != isRightEdge()) {
        throw new DBException.DataCorruption("Next link inconsistent: " + this);
      }

      if (valser == null)
        return;

      int valsSize = valser.valueArraySize(vals);

      if (keyser != null && (keysLen(keyser) != valsSize + 2)) {
        throw new DBException.DataCorruption("Inconsistent vals size: " + this);
      }
      //$DELAY$

      for (int i = 0; i < valsSize; i++) {
        Object val = valser.valueArrayGet(vals, i);
        if (val == null)
          throw new DBException.DataCorruption("Val is null: " + this);
      }

    }

    @Override
    public LeafNode copyAddKey(BTreeKeySerializer keyser, Serializer valser, int pos, Object newKey, long newChild, Object newValue) {
      Object keys2 = keyser.putKey(keys, pos - leftEdgeInc(), newKey);
      //$DELAY$
      Object vals2 = valser.valueArrayPut(vals, pos - 1, newValue);
      //$DELAY$
      return new LeafNode(keys2, isLeftEdge(), isRightEdge(), false, vals2, next);
    }

    @Override
    public LeafNode copySplitRight(BTreeKeySerializer keyser, Serializer valser, int splitPos) {
      int keylen = keyser.length(keys);
      Object keys2 = keyser.copyOfRange(keys, splitPos - leftEdgeInc(), keylen);
      //$DELAY$
      Object vals2 = valser.valueArrayCopyOfRange(vals, splitPos, valser.valueArraySize(vals));
      //$DELAY$
      return new LeafNode(keys2, false, isRightEdge(), false, vals2, next);
    }

    @Override
    public LeafNode copySplitLeft(BTreeKeySerializer keyser, Serializer valser, int splitPos, long newNext) {
      int keypos = splitPos + 1 - leftEdgeInc();
      Object keys2 = keyser.copyOfRange(keys, 0, keypos);
      //clone end value
      Object endkey = keyser.getKey(keys2, keypos - 1);
      keys2 = keyser.putKey(keys2, keypos, endkey);
      //$DELAY$
      Object vals2 = valser.valueArrayCopyOfRange(vals, 0, splitPos);
      //$DELAY$
      //TODO check high/low keys overlap
      return new LeafNode(keys2, isLeftEdge(), false, false, vals2, newNext);
    }

    @Override
    public int valSize(Serializer valueSerializer) {
      return valueSerializer.valueArraySize(vals);
    }

    @Override
    public int childArrayLength() {
      return -1;
    }

    public LeafNode copyChangeValue(Serializer valser, int pos, Object value) {
      Object vals2 = valser.valueArrayUpdateVal(vals, pos - 1, value);
      //$DELAY$
      return new LeafNode(keys, isLeftEdge(), isRightEdge(), false, vals2, next);
    }

    public LeafNode copyRemoveKey(BTreeKeySerializer keyser, Serializer valser, int pos) {
      int keyPos = pos - leftEdgeInc();
      Object keys2 = keyser.deleteKey(keys, keyPos);
      //$DELAY$
      Object vals2 = valser.valueArrayDeleteValue(vals, pos);
      //$DELAY$
      return new LeafNode(keys2, isLeftEdge(), isRightEdge(), false, vals2, next);
    }

    public LeafNode copyClear(BTreeKeySerializer keyser, Serializer valser) {
      Object[] keys2 = new Object[2 - leftEdgeInc() - rightEdgeInc()];
      if (!isLeftEdge())
        keys2[0] = key(keyser, 0);
      //$DELAY$
      if (!isRightEdge())
        keys2[1 - leftEdgeInc()] = highKey(keyser);
      //$DELAY$
      return new LeafNode(keyser.arrayToKeys(keys2), isLeftEdge(), isRightEdge(), false, valser.valueArrayEmpty(), next);
    }
  }


  protected final Serializer<BNode> nodeSerializer;

  protected static final class NodeSerializer<A, B> extends Serializer<BNode> {

    protected static final int LEAF_MASK = 1 << 15;
    protected static final int LEFT_SHIFT = 14;
    protected static final int LEFT_MASK = 1 << LEFT_SHIFT;
    protected static final int RIGHT_SHIFT = 13;
    protected static final int RIGHT_MASK = 1 << RIGHT_SHIFT;
    protected static final int SIZE_MASK = RIGHT_MASK - 1;


    protected final boolean hasValues;
    protected final boolean valsOutsideNodes;
    protected final BTreeKeySerializer keySerializer;
    protected final Serializer<Object> valueSerializer;
    protected final int numberOfNodeMetas;

    public NodeSerializer(boolean valsOutsideNodes, BTreeKeySerializer keySerializer, Serializer valueSerializer, int numberOfNodeMetas) {
      if (keySerializer == null)
        throw new NullPointerException("keySerializer not set");
      this.hasValues = valueSerializer != null;
      this.valsOutsideNodes = valsOutsideNodes;
      this.keySerializer = keySerializer;
      this.valueSerializer = hasValues ?
              (valsOutsideNodes ? new ValRefSerializer() : valueSerializer) :
              BOOLEAN;
      this.numberOfNodeMetas = numberOfNodeMetas;
    }

    @Override
    public void serialize(DataOutput out, BNode value) throws IOException {
      final boolean isLeaf = value.isLeaf();

      //check node integrity in paranoid mode
      if (CC.ASSERT) {
        value.checkStructure(keySerializer, valueSerializer);
      }
      //$DELAY$

      final int header =
              (isLeaf ? LEAF_MASK : 0) |
                      (value.isLeftEdge() ? LEFT_MASK : 0) |
                      (value.isRightEdge() ? RIGHT_MASK : 0) |
                      value.keysLen(keySerializer);

      out.writeShort(header);
      //$DELAY$

      //write node metas, right now this is ignored, but in future it could be used for counted
      // btrees or aggregations
      for (int i = 0; i < numberOfNodeMetas; i++) {
        DataIO.packLong(out, 0);
      }
      //$DELAY$
      //longs go first, so it is possible to reconstruct tree without serializer
      if (isLeaf) {
        DataIO.packLong(out, ((LeafNode) value).next);
      } else {
        serializeChildArray(out, value.childArray());
      }


      if (keySerializer.length(value.keys) > 0)
        keySerializer.serialize(out, value.keys);
      //$DELAY$
      if (isLeaf) {
        valueSerializer.valueArraySerialize(out, ((LeafNode) value).vals);
      }

    }

    protected void serializeChildArray(DataOutput out, Object childArray) throws IOException {
      if (childArray instanceof int[]) {
        int[] cc = (int[]) childArray;
        DataIO.packLong(out, (((long) cc[0]) << 1) | 1L); //pack first value mixed with int flag
        for (int i = 1; i < cc.length; i++) {
          DataIO.packInt(out, cc[i]);
        }
      } else {
        long[] cc = (long[]) childArray;
        DataIO.packLong(out, (cc[0] << 1) | 0); //pack first value mixed with int flag
        for (int i = 1; i < cc.length; i++) {
          DataIO.packLong(out, cc[i]);
        }
      }
    }

    @Override
    public BNode deserialize(DataInput in, int available) throws IOException {
      final int header = in.readUnsignedShort();
      final int size = header & SIZE_MASK;

      //read node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
      for (int i = 0; i < numberOfNodeMetas; i++) {
        DataIO.unpackLong(in);
      }
      //$DELAY$
      //first bite indicates leaf
      final boolean isLeaf = ((header & LEAF_MASK) != 0);
      final int left = (header & LEFT_MASK) >> LEFT_SHIFT;
      final int right = (header & RIGHT_MASK) >> RIGHT_SHIFT;

      DataIO.DataInputInternal in2 = (DataIO.DataInputInternal) in; //TODO fallback option if cast fails
      BNode node;
      if (isLeaf) {
        node = deserializeLeaf(in2, size, left, right);
      } else {
        node = deserializeDir(in2, size, left, right);
      }
      //$DELAY$
      if (CC.ASSERT) {
        node.checkStructure(keySerializer, valueSerializer);
      }
      return node;
    }

    private BNode deserializeDir(final DataIO.DataInputInternal in, final int size, final int left, final int right) throws IOException {
      Object child;
      long firstChild = in.unpackLong();
      if ((firstChild & 1) == 0) {
        //deserialize as long[]
        long[] child_ = new long[size];
        child = child_;
        child_[0] = firstChild >>> 1;
        in.unpackLongArray(child_, 1, size);
      } else {
        //deserialize as long[]
        int[] child_ = new int[size];
        child = child_;
        child_[0] = (int) (firstChild >>> 1);
        in.unpackIntArray(child_, 1, size);
      }

      int keysize = size - left - right;
      //$DELAY$
      final Object keys = keysize == 0 ?
              keySerializer.emptyKeys() :
              keySerializer.deserialize(in, keysize);
      //$DELAY$
      return new DirNode(keys, left != 0, right != 0, false, child);
    }

    private BNode deserializeLeaf(final DataIO.DataInputInternal in, final int size, final int left,
                                  final int right) throws IOException {
      final long next = in.unpackLong();
      int keysize = size - left - right;
      //$DELAY$
      final Object keys = keysize == 0 ?
              keySerializer.emptyKeys() :
              keySerializer.deserialize(in, keysize);

      //$DELAY$
      Object vals = valueSerializer.valueArrayDeserialize(in, size - 2);
      return new LeafNode(keys, left != 0, right != 0, false, vals, next);
    }


    @Override
    public boolean isTrusted() {
      return keySerializer.isTrusted() && valueSerializer.isTrusted();
    }
  }


  /**
   * Constructor used to create new BTreeMap.
   *
   * @param engine            used for persistence
   * @param closeEngine       if this object was created without DB. If true shutdown everything on close method, otherwise DB takes care of shutdown
   * @param rootRecidRef      reference to root recid
   * @param maxNodeSize       maximal BTree Node size. Node will split if number of entries is higher
   * @param valsOutsideNodes  Store Values outside of BTree Nodes in separate record?
   * @param counterRecid      recid under which {@code Atomic.Long} is stored, or {@code 0} for no counter
   * @param keySerializer     Serializer used for keys. May be null for default value.
   * @param valueSerializer   Serializer used for values. May be null for default value
   * @param numberOfNodeMetas number of meta records associated with each BTree node
   */
  public BTreeMap(
          Engine engine,
          boolean closeEngine,
          long rootRecidRef,
          int maxNodeSize,
          boolean valsOutsideNodes,
          long counterRecid,
          BTreeKeySerializer keySerializer,
          final Serializer<V> valueSerializer,
          int numberOfNodeMetas
  ) {
    this.closeEngine = closeEngine;

    if (maxNodeSize % 2 != 0)
      throw new IllegalArgumentException("maxNodeSize must be dividable by 2");
    if (maxNodeSize < 6)
      throw new IllegalArgumentException("maxNodeSize too low");
    if ((maxNodeSize & NodeSerializer.SIZE_MASK) != maxNodeSize)
      throw new IllegalArgumentException("maxNodeSize too high");
    if (rootRecidRef <= 0 || counterRecid < 0 || numberOfNodeMetas < 0)
      throw new IllegalArgumentException();
    if (keySerializer == null)
      throw new NullPointerException();

    this.rootRecidRef = rootRecidRef;
    this.hasValues = valueSerializer != null;
    this.valsOutsideNodes = valsOutsideNodes;
    this.engine = engine;
    this.maxNodeSize = maxNodeSize;
    this.numberOfNodeMetas = numberOfNodeMetas;

    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer != null ? valueSerializer : (Serializer<V>) Serializer.BOOLEAN;
    entrySet = new EntrySet(this, this.valueSerializer);

    this.nodeSerializer = new NodeSerializer(valsOutsideNodes, keySerializer, valueSerializer, numberOfNodeMetas);

    this.keySet = new KeySet(this, hasValues);
    //$DELAY$

    if (counterRecid != 0) {
      this.counter = new Atomic.Long(engine, counterRecid);
      Bind.size(this, counter);
    } else {
      this.counter = null;
    }

    //load left edge refs
    ArrayList leftEdges2 = new ArrayList<Long>();
    long r = engine.get(rootRecidRef, Serializer.RECID);
    for (; ; ) {
      if (CC.ASSERT && r <= 0)
        throw new DBException.DataCorruption("wrong recid");

      //$DELAY$
      BNode n = engine.get(r, nodeSerializer);
      leftEdges2.add(r);
      if (n.isLeaf())
        break;
      r = n.child(0);
    }
    //$DELAY$
    Collections.reverse(leftEdges2);
    leftEdges = Collections.synchronizedList(leftEdges2);
  }

  /**
   * creates empty root node and returns recid of its reference
   */
  static protected long createRootRef(Engine engine, BTreeKeySerializer keySer, Serializer valueSer, int numberOfNodeMetas) {
    Object emptyArray = valueSer != null ?
            valueSer.valueArrayEmpty() :
            Serializer.BOOLEAN.valueArrayEmpty();
    final LeafNode emptyRoot = new LeafNode(keySer.emptyKeys(), true, true, false, emptyArray, 0);
    //empty root is serializer simpler way, so we can use dummy values
    long rootRecidVal = engine.put(emptyRoot, new NodeSerializer(false, keySer, valueSer, numberOfNodeMetas));
    return engine.put(rootRecidVal, Serializer.RECID);
  }

  public List<V> getAll(Object key) {
    return getAllInner(key);
  }

  @Override
  public V get(Object key) {
    return (V) get(key, true);
  }

  private List<V> getAllInner(Object key) {
    if (key == null) throw new NullPointerException();
    K v = (K) key;
    long current = engine.get(rootRecidRef, Serializer.RECID); //get root
    //$DELAY$
    BNode A = engine.get(current, nodeSerializer);

    //dive until  leaf
    while (!A.isLeaf()) {
      //$DELAY$
      current = nextDir((DirNode) A, v);
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }

    for (; ; ) {
      int pos = keySerializer.findChildren2(A, key);
      //$DELAY$
      if (pos > 0 && pos != A.keysLen(keySerializer) - 1) {
        //found
        Object val = A.val(pos - 1, valueSerializer);
        //$DELAY$
        List<V> retList = valExpandList(val);
        return retList;
      } else if (pos <= 0 && -pos - 1 != A.keysLen(keySerializer) - 1) {
        //$DELAY$
        //not found
        return null;
      } else {
        //move to next link
        current = A.next();
        //$DELAY$
        if (current == 0) {
          return null;
        }
        A = engine.get(current, nodeSerializer);
      }
    }
  }

  protected Object get(Object key, boolean expandValue) {
    if (key == null) throw new NullPointerException();
    K v = (K) key;
    long current = engine.get(rootRecidRef, Serializer.RECID); //get root
    //$DELAY$
    BNode A = engine.get(current, nodeSerializer);

    //dive until  leaf
    while (!A.isLeaf()) {
      //$DELAY$
      current = nextDir((DirNode) A, v);
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }

    for (; ; ) {
      int pos = keySerializer.findChildren2(A, key);
      //$DELAY$
      if (pos > 0 && pos != A.keysLen(keySerializer) - 1) {
        //found
        Object val = A.val(pos - 1, valueSerializer);
        //$DELAY$
        if (expandValue)
          val = valExpand(val);
        return val;
      } else if (pos <= 0 && -pos - 1 != A.keysLen(keySerializer) - 1) {
        //$DELAY$
        //not found
        return null;
      } else {
        //move to next link
        current = A.next();
        //$DELAY$
        if (current == 0) {
          return null;
        }
        A = engine.get(current, nodeSerializer);
      }
    }

  }

  protected List<V> valExpandList(Object ret) {
    List<V> retList = new LinkedList<>();
    if (valsOutsideNodes && ret != null) {
      for (int i = 0; i < ((ValRef) ret).recids.size(); i++) {
        long recid = ((ValRef) ret).recids.get(i);
        //$DELAY$
        retList.add((V) engine.get(recid, valueSerializer));
      }
    }
    return retList;
  }

  protected V valExpand(Object ret) {
    if (valsOutsideNodes && ret != null) {
      long recid = ((ValRef) ret).recids.get(0);
      //$DELAY$
      ret = engine.get(recid, valueSerializer);
    }
    return (V) ret;
  }

  protected final long nextDir(DirNode d, Object key) {
    int pos = keySerializer.findChildren(d, key) - 1;
    //$DELAY$
    if (pos < 0)
      pos = 0;
    return d.child(pos);
  }



  @Override
  public V put(K key, V value) {
    if (key == null || value == null) throw new NullPointerException();
    return put2(key, value, false, false);
  }

  /**
   * update the existing ValRef with the recordID of new value
   * if ValRef has been reached the bufferoverflow point, we need to redistribute the record IDs
   * to the ValRef with the new hash keys, otherwise, we directly update the ValRef by appending
   * the new record id
   */
  private ValRef doUpdateOldValueRef(Object oldValue,
                                     long valueRecId,
                                     long nodeRecId,
                                     K searchKey) {
    ValRef oldValueRef = (ValRef) oldValue;
    /*System.out.println(Thread.currentThread().getName() + " updates node " + nodeRecId +
            ", value " + oldValueRef);*/
    oldValueRef.appendNewRecId(valueRecId);
    int currentLevel = oldValueRef.currentLevel;
    if (oldValueRef.recids.size() >= BTreeDatabase.btreeMaximumNode() &&
            currentLevel < BTreeDatabase.btreeCompareGroupNum() - 1) {
      /*
      System.out.print(Thread.currentThread().getName() + " redistributing oldValue: ");
      for (int i = 0; i < oldValueRef.recids.size(); i++) {
        System.out.print(oldValueRef.recids.get(i) + " ");
      }
      System.out.println(" at table " + tableId + " at node " + nodeRecId);*/
      // save the current node
      List<Long> recIdsToRedistribution = new LinkedList<Long>();//oldValueRef.recids.toArray();
      for (int i = 0; i < oldValueRef.recids.size(); i++) {
        recIdsToRedistribution.add(oldValueRef.recids.get(i));
      }
      oldValueRef.recids.clear();
      BNode A = engine.get(nodeRecId, nodeSerializer);
      int pos = keySerializer.findChildren(A, searchKey);
      updateLeafNode(nodeRecId, A, pos, oldValueRef);
      // redistribution
      for (int i = 0; i < recIdsToRedistribution.size(); i++) {
        long existingValRecId = recIdsToRedistribution.get(i);
        LSHBTreeVal btreeVal = (LSHBTreeVal) engine.get(existingValRecId, valueSerializer);
        long fullHash = btreeVal.hash;
        int shiftBits = (BTreeDatabase.btreeCompareGroupNum() - 1 -
                (currentLevel + 1)) * BTreeDatabase.btreeCompareGroupLength();
        Long nextLevelHash = calculateNextLevelHash(fullHash, currentLevel);
        Long originalHash = fullHash >>> (shiftBits + BTreeDatabase.btreeCompareGroupLength());
        /*
        System.out.println(Thread.currentThread().getName() + " redistributing " + existingValRecId +
                " at level " + currentLevel + " with hash value " + nextLevelHash + " at table " +
                tableId + ", shift bits: " + shiftBits + " at node " + nodeRecId + ", original hash: " +
                originalHash);
                */
        appendExistingRecId((K) nextLevelHash, existingValRecId, currentLevel + 1);
      }
    } else {
      // directly append new recid
      /*
      System.out.println(Thread.currentThread().getName() + " directly add " + valueRecId +
              " at level " + currentLevel +
              " at table " + tableId  + " at node " + nodeRecId);
              */
    }
    return oldValueRef;
  }

  private long calculateNextLevelHash(long completeHash, int currentLevel) {
    int totalHashBits = BTreeDatabase.btreeCompareGroupNum() *
            BTreeDatabase.btreeCompareGroupLength();
    int nextLevel = currentLevel + 1;
    if (nextLevel >= BTreeDatabase.btreeCompareGroupNum()) {
      // compose the level bits by shifting to the left for 32 bits
      long levelBits = nextLevel << totalHashBits;
      return levelBits | completeHash;
    } else {
      int nextShiftingLength = (BTreeDatabase.btreeCompareGroupNum() - 1 -
              nextLevel) * BTreeDatabase.btreeCompareGroupLength();
      long hashBits = completeHash >>> nextShiftingLength;
      long levelBits = (nextLevel + 1) << (totalHashBits - nextShiftingLength);
      return hashBits | levelBits;
    }
  }

  private V putWithRedistribution(final K key, final V value2,
                                  final boolean ifAppend, int currentLevel) {
    K v = key;

    int stackPos = -1;
    long[] stackVals = new long[4];

    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    long current = rootRecid;
    //$DELAY$
    BNode A = engine.get(current, nodeSerializer);
    while (!A.isLeaf()) {
      //$DELAY$
      long t = current;
      current = nextDir((DirNode) A, v);
      //$DELAY$
      if (CC.ASSERT && !(current > 0))
        throw new DBException.DataCorruption("wrong recid");
      //if is not link
      if (current != A.next()) {
        //stack push t
        stackPos++;
        if (stackVals.length == stackPos) //grow if needed
          stackVals = Arrays.copyOf(stackVals, stackVals.length * 2);
        //$DELAY$
        stackVals[stackPos] = t;
      }
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }
    int level = 0;

    long p = 0;
    try {
      while (true) {
        //$DELAY$
        boolean found;
        do {
          //$DELAY$
          lock(nodeLocks, current);
          //$DELAY$
          found = true;
          A = engine.get(current, nodeSerializer);
          int pos = keySerializer.findChildren(A, v);
          //check if keys is already in tree
          //$DELAY$
          if (pos < A.keysLen(keySerializer) - 1 && v != null &&
                  A.key(keySerializer, pos) != null && //TODO A.key(pos]!=null??
                  0 == A.compare(keySerializer, pos, v)) {
            //$DELAY$
            //yes key is already in tree
            Object oldVal = A.val(pos - 1, valueSerializer);
            if (oldVal == null) {
              System.out.println("fetch an null value in record " + current + " at pos " + (pos - 1));
              if (!A.isLeaf()) {
                System.out.println("FAULT: A shall be a leaf node");
                System.exit(1);
              }
            }
            //insert new
            V value = value2;
            if (valsOutsideNodes) {
              long recid = engine.put(value2, valueSerializer);
              if (!ifAppend) {
                //$DELAY$
                List<Long> l = new LinkedList<Long>();
                l.add(recid);
                value = (V) new ValRef(l);
              } else {
                value = (V) updateOldRef((ValRef) oldVal, recid, current, currentLevel, v);
              }
            }
            if (value != null) {
              //$DELAY$
              updateLeafNode(current, A, pos, value);
            } else {
              System.out.println("FAULT: value is null");
            }
            //$DELAY$
            //already in here
            V ret;
            ValRef oldValRef = (ValRef) oldVal;
            if (oldValRef.recids.size() > 0) {
              ret = valExpand(oldVal);
            } else {
              ret = null;
            }
            notify(key, ret, value2);
            unlock(nodeLocks, current);
            //$DELAY$
            // if (CC.ASSERT) assertNoLocks(nodeLocks);
            return ret;
          }

          //if v > highvalue(a)
          if (!A.isRightEdge() && A.compare(keySerializer, A.keysLen(keySerializer) - 1, v) < 0) {
            //$DELAY$
            //follow link until necessary
            unlock(nodeLocks, current);
            found = false;
            //$DELAY$
            int pos2 = keySerializer.findChildren(A, v);
            while (A != null && pos2 == A.keysLen(keySerializer)) {
              //TODO lock?
              long next = A.next();
              //$DELAY$
              if (next == 0) break;
              current = next;
              A = engine.get(current, nodeSerializer);
              //$DELAY$
              pos2 = keySerializer.findChildren(A, v);
            }
          }
        } while (!found);

        V value = value2;
        long recid = 0;
        if (valsOutsideNodes) {
          recid = engine.put(value2, valueSerializer);
          List<Long> l = new LinkedList<Long>();
          l.add(recid);
          //$DELAY$
          ValRef newValRef = new ValRef(l);
          newValRef.currentLevel = currentLevel;
          /*
          System.out.println(Thread.currentThread().getName() +
                  " add new rec " + recid + " at level " + currentLevel + " at table " +
                  tableId + " with key " + key);*/
          value = (V) newValRef;
        }

        int pos = keySerializer.findChildren(A, v);
        //$DELAY$
        A = A.copyAddKey(keySerializer, valueSerializer, pos, v, p, value);
        //$DELAY$
        // can be new item inserted into A without splitting it?
        if (A.keysLen(keySerializer) - (A.isLeaf() ? 1 : 0) < maxNodeSize) {
          //$DELAY$
          //if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            //throw new AssertionError();
          engine.update(current, A, nodeSerializer);

          notify(key, null, value2);
          /*
          System.out.println(Thread.currentThread().getName() + " add value " + recid +
                  " without splitting node at node " + current + " with hash key " + key +
                  " at level " + currentLevel);*/
          //$DELAY$
          unlock(nodeLocks, current);
          // if (CC.ASSERT) assertNoLocks(nodeLocks);
          return null;
        } else {
          //node is not safe, it requires splitting

          final int splitPos = A.keysLen(keySerializer) / 2;
          //$DELAY$
          BNode B = A.copySplitRight(keySerializer, valueSerializer, splitPos);
          //$DELAY$
          long q = engine.put(B, nodeSerializer);
          // System.out.println("generate node " + q + " from " + current + " when inserting " + recid);
          A = A.copySplitLeft(keySerializer, valueSerializer, splitPos, q);
          //$DELAY$
          //if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            //throw new AssertionError();
          engine.update(current, A, nodeSerializer);

          if ((current != rootRecid)) { //is not root
            unlock(nodeLocks, current);
            // System.out.println("split " + current + " when inserting " + recid);
            p = q;
            v = (K) A.highKey(keySerializer);
            //$DELAY$
            level = level + 1;
            if (stackPos != -1) { //if stack is not empty
              current = stackVals[stackPos--];
            } else {
              //current := the left most node at level
              current = leftEdges.get(level - 1);
            }
            //$DELAY$
            if (CC.ASSERT && !(current > 0))
              throw new DBException.DataCorruption("wrong recid");
          } else {
            // System.out.println("split " + current + "(root) when inserting " + recid);
            splitRoot(current, q, A);
            return null;
          }
        }
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      unlockAll(nodeLocks);
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      unlockAll(nodeLocks);
      throw new RuntimeException(e);
    }
  }

  protected V put2(final K key, final V value2, final boolean putOnlyIfAbsent,
                   final boolean ifAppend) {
    K v = key;

    int stackPos = -1;
    long[] stackVals = new long[4];

    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    long current = rootRecid;
    //$DELAY$
    BNode A = engine.get(current, nodeSerializer);
    while (!A.isLeaf()) {
      //$DELAY$
      long t = current;
      current = nextDir((DirNode) A, v);
      //$DELAY$
      if (CC.ASSERT && !(current > 0))
        throw new DBException.DataCorruption("wrong recid");
      //if is not link
      if (current != A.next()) {
        //stack push t
        stackPos++;
        if (stackVals.length == stackPos) //grow if needed
          stackVals = Arrays.copyOf(stackVals, stackVals.length * 2);
        //$DELAY$
        stackVals[stackPos] = t;
      }
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }
    int level = 0;

    long p = 0;
    try {
      while (true) {
        //$DELAY$
        boolean found;
        do {
          //$DELAY$
          lock(nodeLocks, current);
          //$DELAY$
          found = true;
          A = engine.get(current, nodeSerializer);
          int pos = keySerializer.findChildren(A, v);
          //check if keys is already in tree
          //$DELAY$
          if (pos < A.keysLen(keySerializer) - 1 && v != null &&
                  A.key(keySerializer, pos) != null && //TODO A.key(pos]!=null??
                  0 == A.compare(keySerializer, pos, v)) {
            //$DELAY$
            //yes key is already in tree
            // nan zhu: the implementation of val is calling valueArrayGet
            // which does not need to call valueSerializer in fact, as a result, we can do type
            // convertion to either value (in case of valueOutsideNodes as false) or valref (in
            // case of valueOutsideNodes as true)
            Object oldVal = A.val(pos - 1, valueSerializer);
            //$DELAY$
            if (putOnlyIfAbsent) {
              //is not absent, so quit
              unlock(nodeLocks, current);
              if (CC.ASSERT) assertNoLocks(nodeLocks);
              return valExpand(oldVal);
            }

            //insert new
            V value = value2;
            if (valsOutsideNodes) {
              long recid = engine.put(value2, valueSerializer);
              if (!ifAppend) {
                //$DELAY$
                List<Long> l = new LinkedList<Long>();
                l.add(recid);
                value = (V) new ValRef(l);
              } else {
                ((ValRef) oldVal).appendNewRecId(recid);
                value = (V) oldVal;
              }
            }

            System.out.println("A is leaf node: " + A.isLeaf());

            //$DELAY$
            A = ((LeafNode) A).copyChangeValue(valueSerializer, pos, value);
            if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
              throw new AssertionError();
            engine.update(current, A, nodeSerializer);
            //$DELAY$
            //already in here
            V ret = valExpand(oldVal);
            notify(key, ret, value2);
            unlock(nodeLocks, current);
            //$DELAY$
            if (CC.ASSERT) assertNoLocks(nodeLocks);
            return ret;
          }

          //if v > highvalue(a)
          if (!A.isRightEdge() && A.compare(keySerializer, A.keysLen(keySerializer) - 1, v) < 0) {
            //$DELAY$
            //follow link until necessary
            unlock(nodeLocks, current);
            found = false;
            //$DELAY$
            int pos2 = keySerializer.findChildren(A, v);
            while (A != null && pos2 == A.keysLen(keySerializer)) {
              //TODO lock?
              long next = A.next();
              //$DELAY$
              if (next == 0) break;
              current = next;
              A = engine.get(current, nodeSerializer);
              //$DELAY$
              pos2 = keySerializer.findChildren(A, v);
            }
          }
        } while (!found);

        V value = value2;
        if (valsOutsideNodes) {
          long recid = engine.put(value2, valueSerializer);
          List<Long> l = new LinkedList<Long>();
          l.add(recid);
          //$DELAY$
          value = (V) new ValRef(l);
        }

        int pos = keySerializer.findChildren(A, v);
        //$DELAY$
        boolean AIsLeafBefore = A.isLeaf();
        System.out.println("A is leaf node: " + A.isLeaf() + " (before adding key)" +
                " thread: " + AIsLeafBefore);
        A = A.copyAddKey(keySerializer, valueSerializer, pos, v, p, value);
        boolean AIsLeafAfter = A.isLeaf();
        System.out.println("A is leaf node: " + A.isLeaf() + " (after adding key)" +
                " thread: " + AIsLeafAfter);
        if (AIsLeafAfter != AIsLeafBefore) {
          System.out.println("FAULT: inconsistent A's type, before " + AIsLeafBefore + " after " +
            AIsLeafAfter + " thread " + Thread.currentThread().getName());
          System.exit(1);
        }
        //$DELAY$
        // can be new item inserted into A without splitting it?
        if (A.keysLen(keySerializer) - (A.isLeaf() ? 1 : 0) < maxNodeSize) {
          //$DELAY$
          if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            throw new AssertionError();
          engine.update(current, A, nodeSerializer);

          notify(key, null, value2);
          //$DELAY$
          unlock(nodeLocks, current);
          if (CC.ASSERT) assertNoLocks(nodeLocks);
          return null;
        } else {
          //node is not safe, it requires splitting

          final int splitPos = A.keysLen(keySerializer) / 2;
          //$DELAY$
          BNode B = A.copySplitRight(keySerializer, valueSerializer, splitPos);
          //$DELAY$
          long q = engine.put(B, nodeSerializer);
          A = A.copySplitLeft(keySerializer, valueSerializer, splitPos, q);
          //$DELAY$
          if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            throw new AssertionError();
          engine.update(current, A, nodeSerializer);

          if ((current != rootRecid)) { //is not root
            unlock(nodeLocks, current);
            p = q;
            v = (K) A.highKey(keySerializer);
            //$DELAY$
            level = level + 1;
            if (stackPos != -1) { //if stack is not empty
              current = stackVals[stackPos--];
            } else {
              //current := the left most node at level
              current = leftEdges.get(level - 1);
            }
            //$DELAY$
            if (CC.ASSERT && !(current > 0))
              throw new DBException.DataCorruption("wrong recid");
          } else {
            Object rootChild =
                    (current < Integer.MAX_VALUE && q < Integer.MAX_VALUE) ?
                            new int[]{(int) current, (int) q, 0} :
                            new long[]{current, q, 0};


            BNode R = new DirNode(
                    keySerializer.arrayToKeys(new Object[]{A.highKey(keySerializer)}),
                    true, true, false,
                    rootChild);
            //$DELAY$
            unlock(nodeLocks, current);
            //$DELAY$
            lock(nodeLocks, rootRecidRef);

            //$DELAY$
            long newRootRecid = engine.put(R, nodeSerializer);
            //$DELAY$
            if (CC.ASSERT && !(nodeLocks.get(rootRecidRef).isHeldByCurrentThread()))
              throw new AssertionError();

            leftEdges.add(newRootRecid);
            //TODO there could be a race condition between leftEdges  update and rootRecidRef update. Investigate!
            //$DELAY$

            engine.update(rootRecidRef, newRootRecid, Serializer.RECID);

            notify(key, null, value2);
            //$DELAY$
            unlock(nodeLocks, rootRecidRef);
            //$DELAY$
            if (CC.ASSERT) assertNoLocks(nodeLocks);
            //$DELAY$
            return null;
          }
        }
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      unlockAll(nodeLocks);
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      unlockAll(nodeLocks);
      throw new RuntimeException(e);
    }
  }


  protected static class BTreeIterator {
    final BTreeMap m;

    LeafNode currentLeaf;
    Object lastReturnedKey;
    int currentPos;
    final Object hi;
    final boolean hiInclusive;

    /**
     * unbounded iterator
     */
    BTreeIterator(BTreeMap m) {
      this.m = m;
      hi = null;
      hiInclusive = false;
      pointToStart();
    }

    /**
     * bounder iterator, args may be null for partially bounded
     */
    BTreeIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      this.m = m;
      if (lo == null) {
        //$DELAY$
        pointToStart();
      } else {
        //$DELAY$
        Fun.Pair<Integer, LeafNode> l = m.findLargerNode(lo, loInclusive);
        currentPos = l != null ? l.a : -1;
        currentLeaf = l != null ? l.b : null;
      }
      this.hi = hi;
      this.hiInclusive = hiInclusive;
      //$DELAY$
      if (hi != null && currentLeaf != null) {
        //check in bounds
        int c = currentLeaf.compare(m.keySerializer, currentPos, hi);
        if (c > 0 || (c == 0 && !hiInclusive)) {
          //out of high bound
          currentLeaf = null;
          currentPos = -1;
          //$DELAY$
        }
      }

    }


    private void pointToStart() {
      //find left-most leaf
      final long rootRecid = m.engine.get(m.rootRecidRef, Serializer.RECID);
      BNode node = (BNode) m.engine.get(rootRecid, m.nodeSerializer);
      //$DELAY$
      while (!node.isLeaf()) {
        //$DELAY$
        node = (BNode) m.engine.get(node.child(0), m.nodeSerializer);
      }
      currentLeaf = (LeafNode) node;
      currentPos = 1;
      //$DELAY$
      while (currentLeaf.keysLen(m.keySerializer) == 2) {
        //follow link until leaf is not empty
        if (currentLeaf.next == 0) {
          //$DELAY$
          currentLeaf = null;
          return;
        }
        //$DELAY$
        currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
      }
    }


    public boolean hasNext() {
      return currentLeaf != null;
    }

    public void remove() {
      if (lastReturnedKey == null) throw new IllegalStateException();
      m.remove(lastReturnedKey);
      //$DELAY$
      lastReturnedKey = null;
    }

    protected void advance() {
      if (currentLeaf == null) return;
      lastReturnedKey = currentLeaf.key(m.keySerializer, currentPos);
      currentPos++;
      //$DELAY$
      if (currentPos == currentLeaf.keysLen(m.keySerializer) - 1) {
        //move to next leaf
        if (currentLeaf.next == 0) {
          currentLeaf = null;
          currentPos = -1;
          return;
        }
        //$DELAY$
        currentPos = 1;
        currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
        while (currentLeaf.keysLen(m.keySerializer) == 2) {
          if (currentLeaf.next == 0) {
            currentLeaf = null;
            currentPos = -1;
            return;
          }
          currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
          //$DELAY$
        }
      }
      if (hi != null && currentLeaf != null) {
        //check in bounds
        int c = currentLeaf.compare(m.keySerializer, currentPos, hi);
        if (c > 0 || (c == 0 && !hiInclusive)) {
          //$DELAY$
          //out of high bound
          currentLeaf = null;
          currentPos = -1;
        }
      }
    }
  }


  protected static class BTreeDescendingIterator {
    final BTreeMap m;

    LeafNode currentLeaf;
    Object lastReturnedKey;
    int currentPos;
    final Object lo;
    final boolean loInclusive;

    /**
     * unbounded iterator
     */
    BTreeDescendingIterator(BTreeMap m) {
      this.m = m;
      lo = null;
      loInclusive = false;
      pointToStart();
    }

    /**
     * bounder iterator, args may be null for partially bounded
     */
    BTreeDescendingIterator(
            BTreeMap m,
            Object lo,
            boolean loInclusive,
            Object hi,
            boolean hiInclusive) {
      this.m = m;
      if (hi == null) {
        //$DELAY$
        pointToStart();
      } else {
        //$DELAY$
        Fun.Pair<Integer, LeafNode> l = m.findSmallerNode(hi, hiInclusive);
        currentPos = l != null ? l.a : -1;
        currentLeaf = l != null ? l.b : null;
      }
      this.lo = lo;
      this.loInclusive = loInclusive;
      //$DELAY$
      if (lo != null && currentLeaf != null) {
        //check in bounds
        int c = -currentLeaf.compare(m.keySerializer, currentPos, lo);
        if (c > 0 || (c == 0 && !loInclusive)) {
          //out of high bound
          currentLeaf = null;
          currentPos = -1;
          //$DELAY$
        }
      }

    }


    private void pointToStart() {
      //find right-most leaf
      final long rootRecid = m.engine.get(m.rootRecidRef, Serializer.RECID);
      BNode node = (BNode) m.engine.get(rootRecid, m.nodeSerializer);
      //descend and follow link until possible
      for (; ; ) {
        long next = node.next();
        if (next == 0) {
          if (node.isLeaf()) {
            //end
            currentLeaf = (LeafNode) node;
            int len = currentLeaf.keysLen(m.keySerializer);
            if (len == 2) {
              currentLeaf = null;
              currentPos = -1;
            } else {
              currentPos = len - 2;
            }
            return;
          }
          //follow last children in directory
          Object children = node.childArray();

          next = children instanceof int[] ?
                  ((int[]) children)[((int[]) children).length - 2] :
                  ((long[]) children)[((long[]) children).length - 2];
        }
        node = (BNode) m.engine.get(next, m.nodeSerializer);
      }
    }


    public boolean hasNext() {
      return currentLeaf != null;
    }

    public void remove() {
      if (lastReturnedKey == null) throw new IllegalStateException();
      m.remove(lastReturnedKey);
      //$DELAY$
      lastReturnedKey = null;
    }

    protected void advance() {
      if (currentLeaf == null)
        return;
      lastReturnedKey = currentLeaf.key(m.keySerializer, currentPos);
      currentPos--;
      //$DELAY$
      if (currentPos == 0) {
        //$DELAY$
        Object nextKey = currentLeaf.key(m.keySerializer, 0);
        Fun.Pair<Integer, BNode> prevPair =
                nextKey == null ? null :
                        m.findSmallerNode(nextKey, false);
        if (prevPair == null) {
          currentLeaf = null;
          currentPos = -1;
          return;
        }
        currentLeaf = (LeafNode) prevPair.b;
        currentPos = currentLeaf.keysLen(m.keySerializer) - 2;


        while (currentLeaf.keysLen(m.keySerializer) == 2) {
          if (currentLeaf.next == 0) {
            currentLeaf = null;
            currentPos = -1;
            return;
          }
          currentLeaf = (LeafNode) m.engine.get(currentLeaf.next, m.nodeSerializer);
          //$DELAY$
        }
      }
      if (lo != null && currentLeaf != null) {
        //check in bounds
        int c = -currentLeaf.compare(m.keySerializer, currentPos, lo);
        if (c > 0 || (c == 0 && !loInclusive)) {
          //$DELAY$
          //out of high bound
          currentLeaf = null;
          currentPos = -1;
        }
      }
    }
  }


  @Override
  public V remove(Object key) {
    return removeOrReplace(key, null, null);
  }

  private V removeOrReplace(final Object key, final Object value, final Object putNewValue) {
    if (key == null)
      throw new NullPointerException("null key");
    long current = engine.get(rootRecidRef, Serializer.RECID);

    BNode A = engine.get(current, nodeSerializer);
    //$DELAY$
    while (!A.isLeaf()) {
      //$DELAY$
      current = nextDir((DirNode) A, key);
      A = engine.get(current, nodeSerializer);
    }

    long old = 0;
    try {
      for (; ; ) {
        //$DELAY$
        if (old != 0) {
          //$DELAY$
          unlock(nodeLocks, old);
        }
        //$DELAY$
        lock(nodeLocks, current);

        A = engine.get(current, nodeSerializer);
        //$DELAY$
        int pos = keySerializer.findChildren2(A, key);
//            System.out.println(key+" - "+pos+" - "+A);
        if (pos > 0 && pos != A.keysLen(keySerializer) - 1) {
          //found, delete from node
          //$DELAY$
          Object oldVal = A.val(pos - 1, valueSerializer);
          oldVal = valExpand(oldVal);
          if (value != null && valueSerializer != null && !valueSerializer.equals((V) value, (V) oldVal)) {
            unlock(nodeLocks, current);
            //$DELAY$
            return null;
          }

          Object putNewValueOutside = putNewValue;
          if (putNewValue != null && valsOutsideNodes) {
            //$DELAY$
            long recid = engine.put((V) putNewValue, valueSerializer);
            List<Long> l = new LinkedList<Long>();
            l.add(recid);
            //$DELAY$
            putNewValueOutside = new ValRef(l);
          }

          A = putNewValue != null ?
                  ((LeafNode) A).copyChangeValue(valueSerializer, pos, putNewValueOutside) :
                  ((LeafNode) A).copyRemoveKey(keySerializer, valueSerializer, pos);
          if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            throw new AssertionError();
          //$DELAY$
          engine.update(current, A, nodeSerializer);
          notify((K) key, (V) oldVal, (V) putNewValue);
          unlock(nodeLocks, current);
          return (V) oldVal;
        } else if (pos <= 0 && -pos - 1 != A.keysLen(keySerializer) - 1) {
          //not found
          unlock(nodeLocks, current);
          //$DELAY$
          return null;
        } else {
          //move to next link
          old = current;
          current = A.next();
          //$DELAY$
          if (current == 0) {
            //end reached
            unlock(nodeLocks, old);
            return null;
          }
        }

      }
    } catch (RuntimeException e) {
      unlockAll(nodeLocks);
      throw e;
    } catch (Exception e) {
      unlockAll(nodeLocks);
      throw new RuntimeException(e);
    }
  }


  @Override
  public void clear() {
    boolean hasListeners = modListeners.length > 0;
    long current = engine.get(rootRecidRef, Serializer.RECID);

    BNode A = engine.get(current, nodeSerializer);
    //$DELAY$
    while (!A.isLeaf()) {
      current = A.child(0);
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }

    long old = 0;
    try {
      for (; ; ) {
        //$DELAY$
        //lock nodes
        lock(nodeLocks, current);
        if (old != 0) {
          //$DELAY$
          unlock(nodeLocks, old);
        }
        //$DELAY$
        //notify about deletion
        int size = A.keysLen(keySerializer) - 1;
        if (hasListeners) {
          //$DELAY$
          for (int i = 1; i < size; i++) {
            Object val = (V) A.val(i - 1, valueSerializer);
            val = valExpand(val);
            //$DELAY$
            notify((K) A.key(keySerializer, i), (V) val, null);
          }
        }

        //remove all node content
        A = ((LeafNode) A).copyClear(keySerializer, valueSerializer);
        //$DELAY$
        engine.update(current, A, nodeSerializer);

        //move to next link
        old = current;
        //$DELAY$
        current = A.next();
        if (current == 0) {
          //end reached
          //$DELAY$
          unlock(nodeLocks, old);
          //$DELAY$
          return;
        }
        //$DELAY$
        A = engine.get(current, nodeSerializer);
      }
    } catch (RuntimeException e) {
      unlockAll(nodeLocks);
      throw e;
    } catch (Exception e) {
      unlockAll(nodeLocks);
      throw new RuntimeException(e);
    }

  }


  static class BTreeKeyIterator<K> extends BTreeIterator implements Iterator<K> {

    BTreeKeyIterator(BTreeMap m) {
      super(m);
    }

    BTreeKeyIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      super(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public K next() {
      if (currentLeaf == null) throw new NoSuchElementException();
      K ret = (K) currentLeaf.key(m.keySerializer, currentPos);
      //$DELAY$
      advance();
      //$DELAY$
      return ret;
    }
  }

  static class BTreeValueIterator<V> extends BTreeIterator implements Iterator<V> {

    BTreeValueIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      super(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public V next() {
      if (currentLeaf == null) throw new NoSuchElementException();
      Object ret = currentLeaf.val(currentPos - 1, m.valueSerializer);
      //$DELAY$
      advance();
      //$DELAY$
      return (V) m.valExpand(ret);
    }

  }

  static class BTreeEntryIterator<K, V> extends BTreeIterator implements Iterator<Entry<K, V>> {

    BTreeEntryIterator(BTreeMap m) {
      super(m);
    }

    BTreeEntryIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      super(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public Entry<K, V> next() {
      if (currentLeaf == null) throw new NoSuchElementException();
      K ret = (K) currentLeaf.key(m.keySerializer, currentPos);
      Object val = currentLeaf.val(currentPos - 1, m.valueSerializer);
      //$DELAY$
      advance();
      //$DELAY$
      return m.makeEntry(ret, m.valExpand(val));
    }
  }


  static class BTreeDescendingKeyIterator<K> extends BTreeDescendingIterator implements Iterator<K> {

    BTreeDescendingKeyIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      super(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public K next() {
      if (currentLeaf == null)
        throw new NoSuchElementException();
      K ret = (K) currentLeaf.key(m.keySerializer, currentPos);
      //$DELAY$
      advance();
      //$DELAY$
      return ret;
    }
  }

  static class BTreeDescendingValueIterator<V> extends BTreeDescendingIterator implements Iterator<V> {

    BTreeDescendingValueIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      super(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public V next() {
      if (currentLeaf == null) throw new NoSuchElementException();
      Object ret = currentLeaf.val(currentPos - 1, m.valueSerializer);
      //$DELAY$
      advance();
      //$DELAY$
      return (V) m.valExpand(ret);
    }

  }

  static class BTreeDescendingEntryIterator<K, V> extends BTreeDescendingIterator implements Iterator<Entry<K, V>> {

    BTreeDescendingEntryIterator(BTreeMap m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
      super(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public Entry<K, V> next() {
      if (currentLeaf == null)
        throw new NoSuchElementException();
      K ret = (K) currentLeaf.key(m.keySerializer, currentPos);
      Object val = currentLeaf.val(currentPos - 1, m.valueSerializer);
      //$DELAY$
      advance();
      //$DELAY$
      return m.makeEntry(ret, m.valExpand(val));
    }
  }


  protected Entry<K, V> makeEntry(Object key, Object value) {
    if (CC.ASSERT && !(!(value instanceof ValRef)))
      throw new AssertionError();
    return new SimpleImmutableEntry<K, V>((K) key, (V) value);
  }


  @Override
  public boolean isEmpty() {
    return !keyIterator().hasNext();
  }

  @Override
  public int size() {
    return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
  }

  @Override
  public long sizeLong() {
    if (counter != null)
      return counter.get();

    long size = 0;
    BTreeIterator iter = new BTreeIterator(this);
    //$DELAY$
    while (iter.hasNext()) {
      //$DELAY$
      iter.advance();
      size++;
    }
    return size;
  }

  public long mappingCount() {
    //method added in java 8
    return sizeLong();
  }

  private void splitRoot(long currentNodeRecId, long newlyGeneratedNodeRecId, BNode originalNode)
          throws InterruptedException{
    Object rootChild =
            (currentNodeRecId < Integer.MAX_VALUE && newlyGeneratedNodeRecId < Integer.MAX_VALUE) ?
                    new int[]{(int) currentNodeRecId, (int) newlyGeneratedNodeRecId, 0} :
                    new long[]{currentNodeRecId, newlyGeneratedNodeRecId, 0};


    BNode R = new DirNode(
            keySerializer.arrayToKeys(new Object[]{originalNode.highKey(keySerializer)}),
            true, true, false,
            rootChild);
    //$DELAY$
    unlock(nodeLocks, currentNodeRecId);
    //$DELAY$
    lock(nodeLocks, rootRecidRef);

    //$DELAY$
    long newRootRecid = engine.put(R, nodeSerializer);
    //$DELAY$
    //if (CC.ASSERT && !(nodeLocks.get(rootRecidRef).isHeldByCurrentThread()))
    //throw new AssertionError();

    leftEdges.add(newRootRecid);
    //TODO there could be a race condition between leftEdges  update and rootRecidRef update.
    // Investigate!
    //$DELAY$

    engine.update(rootRecidRef, newRootRecid, Serializer.RECID);

    // notify(key, null, value2);
    //$DELAY$
    unlock(nodeLocks, rootRecidRef);
  }

  private ValRef updateOldRef(ValRef oldRef, long valueRefId, long nodeRecId,
                              int currentLevel, K searchKey) throws Exception {
    if (oldRef.currentLevel == currentLevel) {
      if (oldRef.recids.isEmpty()) {
        // move to nextLevel
        // unlock(nodeLocks, current);
        V value = engine.get(valueRefId, valueSerializer);
        // recalculate the next level hash
        Long newPartialHash = calculateNextLevelHash(((LSHBTreeVal) value).hash,
                currentLevel);
        /*
        System.out.println("meet a intermediate-ValRef at level " + currentLevel +
                " with nextLevelHash " + newPartialHash);*/
        appendExistingRecId((K) newPartialHash, valueRefId, currentLevel + 1);
        return oldRef;
      } else {
        return doUpdateOldValueRef(oldRef, valueRefId, nodeRecId, searchKey);
      }
    } else {
      // the found oldRef is in another currentLevel, we need to recalculate the hash of value
      // and return null to indicate that we shall not add the value to the current ValRef and
      // update the current leaf node
      /*
      V value = engine.get(valueRefId, valueSerializer);
      Long newPartialHash;
      if (currentLevel > oldRef.currentLevel) {
        newPartialHash = calculateNextLevelHash(((LSHBTreeVal) value).hash,
                currentLevel + 1);
        appendExistingRecId((K) newPartialHash, valueRefId, currentLevel + 2);
      } else {
        newPartialHash = calculateNextLevelHash(((LSHBTreeVal) value).hash,
                oldRef.currentLevel);
        appendExistingRecId((K) newPartialHash, valueRefId, oldRef.currentLevel + 1);
      }
      /*
      System.out.println("meet a intermediate-ValRef at unmatched level " + oldRef.currentLevel +
              ", expect level " + currentLevel + " with hash " + newPartialHash);
      return null;
      */
      throw new Exception("currentLevel " + currentLevel + " does not match with the ValRef's" +
              " level " + oldRef.currentLevel + " when inserting record " + valueRefId);
    }
  }

  private BNode updateLeafNode(long currentNodeId, BNode A, int pos, Object value) {
    if (value == null) {
      System.out.println("in updateLeafNode, meet a null value when updating " + currentNodeId);
    }
    A = ((LeafNode) A).copyChangeValue(valueSerializer, pos, value);
    //if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
    //throw new AssertionError();
    engine.update(currentNodeId, A, nodeSerializer);
    return A;
  }

  private void appendExistingRecId(K newKey, long existingRecId, int proposedCurrentLevel) {
    K v = newKey;
    int currentLevel = Math.min(proposedCurrentLevel, BTreeDatabase.btreeCompareGroupNum() - 1);
    int stackPos = -1;
    long[] stackVals = new long[4];

    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    long current = rootRecid;
    //$DELAY$
    BNode A = engine.get(current, nodeSerializer);
    while (!A.isLeaf()) {
      //$DELAY$
      long t = current;
      current = nextDir((DirNode) A, v);
      //$DELAY$
      if (CC.ASSERT && !(current > 0))
        throw new DBException.DataCorruption("wrong recid");
      //if is not link
      if (current != A.next()) {
        //stack push t
        stackPos++;
        if (stackVals.length == stackPos) //grow if needed
          stackVals = Arrays.copyOf(stackVals, stackVals.length * 2);
        //$DELAY$
        stackVals[stackPos] = t;
      }
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }
    int level = 0;

    long p = 0;
    try {
      while (true) {
        //$DELAY$
        boolean found;
        do {
          //$DELAY$
          lock(nodeLocks, current);
          //$DELAY$
          found = true;
          A = engine.get(current, nodeSerializer);
          int pos = keySerializer.findChildren(A, v);
          //check if keys is already in tree
          //$DELAY$
          if (pos < A.keysLen(keySerializer) - 1 && v != null &&
                  A.key(keySerializer, pos) != null && //TODO A.key(pos]!=null??
                  0 == A.compare(keySerializer, pos, v)) {
            //$DELAY$
            //yes key is already in tree
            Object oldVal = A.val(pos - 1, valueSerializer);
            if (oldVal == null) {
              System.out.println("fetch an null value in record " + current + " at pos " +
                      (pos - 1) + " in appendExistingRecId()");
              if (!A.isLeaf()) {
                System.out.println("FAULT: A shall be a leaf node");
                System.exit(1);
              }
            }

            //insert new
            V value = null;
            if (valsOutsideNodes) {
              value = (V) updateOldRef((ValRef) oldVal, existingRecId, current, currentLevel, v);
            } else {
              throw new Exception("appendExistingRecId does not support in valsInsideNodes");
            }

            //$DELAY$
            if (value != null) {
              updateLeafNode(current, A, pos, value);
            } else {
              System.out.println("FAULT: value is null");
            }
            //$DELAY$
            //already in here
            unlock(nodeLocks, current);
            //$DELAY$
            // if (CC.ASSERT) assertNoLocks(nodeLocks);
            return;
          }

          //if v > highvalue(a)
          if (!A.isRightEdge() && A.compare(keySerializer, A.keysLen(keySerializer) - 1, v) < 0) {
            //$DELAY$
            //follow link until necessary
            unlock(nodeLocks, current);
            found = false;
            //$DELAY$
            int pos2 = keySerializer.findChildren(A, v);
            while (A != null && pos2 == A.keysLen(keySerializer)) {
              //TODO lock?
              long next = A.next();
              //$DELAY$
              if (next == 0) break;
              current = next;
              A = engine.get(current, nodeSerializer);
              //$DELAY$
              pos2 = keySerializer.findChildren(A, v);
            }
          }
        } while (!found);

        V value = null;
        if (valsOutsideNodes) {
          List<Long> l = new LinkedList<Long>();
          l.add(existingRecId);
          //$DELAY$
          ValRef newValRef = new ValRef(l);
          newValRef.currentLevel = currentLevel;
          /*
          System.out.println(Thread.currentThread().getName() +
                  " add new rec " + existingRecId + " at level " + currentLevel + " at table " +
                  tableId + " with key " + newKey + " with in appendExistingRecId()");
                  */
          value = (V) newValRef;
        } else {
          throw new Exception("append does not fully support in valuesInsideNodes");
        }

        int pos = keySerializer.findChildren(A, v);
        //$DELAY$
        A = A.copyAddKey(keySerializer, valueSerializer, pos, v, p, value);
        //$DELAY$
        // can be new item inserted into A without splitting it?
        if (A.keysLen(keySerializer) - (A.isLeaf() ? 1 : 0) < maxNodeSize) {
          //$DELAY$
          //if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            //throw new AssertionError();
          engine.update(current, A, nodeSerializer);
          /*
          System.out.println(Thread.currentThread().getName() + " add value " + existingRecId +
                  " without splitting node at node " + current + " with hash key " + newKey +
                  " at level " + currentLevel);*/
          //$DELAY$
          unlock(nodeLocks, current);
          // if (CC.ASSERT) assertNoLocks(nodeLocks);
          return;
        } else {
          //node is not safe, it requires splitting

          final int splitPos = A.keysLen(keySerializer) / 2;
          //$DELAY$
          BNode B = A.copySplitRight(keySerializer, valueSerializer, splitPos);
          //$DELAY$
          long q = engine.put(B, nodeSerializer);
          /*
          System.out.println("generate node " + q + " from " + current +
                  " when inserting " + existingRecId);*/
          A = A.copySplitLeft(keySerializer, valueSerializer, splitPos, q);
          //$DELAY$
          //if (CC.ASSERT && !(nodeLocks.get(current).isHeldByCurrentThread()))
            //throw new AssertionError();
          engine.update(current, A, nodeSerializer);

          if ((current != rootRecid)) { //is not root
            unlock(nodeLocks, current);
            //System.out.println("split " + current + " when inserting " + existingRecId);
            p = q;
            v = (K) A.highKey(keySerializer);
            //$DELAY$
            level = level + 1;
            if (stackPos != -1) { //if stack is not empty
              current = stackVals[stackPos--];
            } else {
              //current := the left most node at level
              current = leftEdges.get(level - 1);
            }
            //$DELAY$
            if (CC.ASSERT && !(current > 0))
              throw new DBException.DataCorruption("wrong recid");
          } else {
            //System.out.println("split " + current + "(root) when inserting " + existingRecId);
            splitRoot(current, q, A);
            return;
          }
        }
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      unlockAll(nodeLocks);
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      unlockAll(nodeLocks);
      throw new RuntimeException(e);
    }
  }

  public void append(K key, V value, int currentLevel) {
    if (key == null || value == null) throw new NullPointerException();
    putWithRedistribution(key, value, true, currentLevel);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    if (key == null || value == null) throw new NullPointerException();
    return put2(key, value, true, false);
  }

  @Override
  public boolean remove(Object key, Object value) {
    if (key == null) throw new NullPointerException();
    return value != null && removeOrReplace(key, value, null) != null;
  }

  @Override
  public boolean replace(final K key, final V oldValue, final V newValue) {
    if (key == null || oldValue == null || newValue == null) throw new NullPointerException();

    return removeOrReplace(key, oldValue, newValue) != null;
  }

  @Override
  public V replace(final K key, final V value) {
    if (key == null || value == null) throw new NullPointerException();

    return removeOrReplace(key, null, value);
  }


  @Override
  public Comparator<? super K> comparator() {
    return keySerializer.comparator();
  }


  @Override
  public Map.Entry<K, V> firstEntry() {
    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    BNode n = engine.get(rootRecid, nodeSerializer);
    //$DELAY$
    while (!n.isLeaf()) {
      //$DELAY$
      n = engine.get(n.child(0), nodeSerializer);
    }
    LeafNode l = (LeafNode) n;
    //follow link until necessary
    while (l.keysLen(keySerializer) == 2) {
      if (l.next == 0) return null;
      //$DELAY$
      l = (LeafNode) engine.get(l.next, nodeSerializer);
    }
    //$DELAY$
    return makeEntry(l.key(keySerializer, 1), valExpand(l.val(0, valueSerializer)));
  }


  @Override
  public K firstKey() {
    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    BNode n = engine.get(rootRecid, nodeSerializer);
    //$DELAY$
    while (!n.isLeaf()) {
      //$DELAY$
      n = engine.get(n.child(0), nodeSerializer);
    }
    LeafNode l = (LeafNode) n;
    //follow link until necessary
    while (l.keysLen(keySerializer) == 2) {
      if (l.next == 0)
        throw new NoSuchElementException();
      //$DELAY$
      l = (LeafNode) engine.get(l.next, nodeSerializer);
    }
    //$DELAY$
    return (K) l.key(keySerializer, 1);
  }


  @Override
  public Entry<K, V> pollFirstEntry() {
    //$DELAY$
    while (true) {
      //$DELAY$
      Entry<K, V> e = firstEntry();
      //$DELAY$
      if (e == null || remove(e.getKey(), e.getValue())) {
        return e;
      }
    }
  }

  @Override
  public Entry<K, V> pollLastEntry() {
    //$DELAY$
    while (true) {
      Entry<K, V> e = lastEntry();
      //$DELAY$
      if (e == null || remove(e.getKey(), e.getValue())) {
        return e;
      }
    }
  }


  protected Entry<K, V> findSmaller(K key, boolean inclusive) {
    if (key == null) throw new NullPointerException();
    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    //$DELAY$
    BNode n = engine.get(rootRecid, nodeSerializer);
    //$DELAY$
    Entry<K, V> k = findSmallerRecur(n, key, inclusive);
    //$DELAY$
    if (k == null || (k.getValue() == null)) return null;
    return k;
  }

  private Entry<K, V> findSmallerRecur(BNode n, K key, boolean inclusive) {
    //TODO optimize comparation in this method
    final boolean leaf = n.isLeaf();
    final int start = leaf ? n.keysLen(keySerializer) - 2 : n.keysLen(keySerializer) - 1;
    final int end = leaf ? 1 : 0;
    final int res = inclusive && leaf ? 1 : 0;
    //$DELAY$
    for (int i = start; i >= end; i--) {
      //$DELAY$
      final Object key2 = n.key(keySerializer, i);
      int comp = (key2 == null) ? -1 : keySerializer.comparator().compare(key2, key);
      if (comp < res) {
        if (leaf) {
          //$DELAY$
          return key2 == null ? null :
                  makeEntry(key2, valExpand(n.val(i - 1, valueSerializer)));
        } else {
          final long recid = n.child(i);
          if (recid == 0) continue;
          BNode n2 = engine.get(recid, nodeSerializer);
          if (n2.isLeaf()) {
            //check if first value is acceptable
            if (n2.keysLen(keySerializer) > 2 &&
                    keySerializer.comparator().compare(
                            n2.key(keySerializer, 1), key) >= (inclusive ? 1 : 0)) {
              continue;
            }
          }
          //$DELAY$
          Entry<K, V> ret = findSmallerRecur(n2, key, inclusive);
          if (ret != null) return ret;
        }
      }
    }

    return null;
  }


  protected Fun.Pair<Integer, BNode> findSmallerNode(K key, boolean inclusive) {
    if (key == null)
      throw new NullPointerException();
    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    //$DELAY$
    BNode n = engine.get(rootRecid, nodeSerializer);
    //$DELAY$
    return findSmallerNodeRecur(n, key, inclusive);
  }

  protected Fun.Pair<Integer, BNode> findSmallerNodeRecur(
          BNode n, K key, boolean inclusive) {
    //TODO optimize comparation in this method
    final boolean leaf = n.isLeaf();
    final int start = leaf ? n.keysLen(keySerializer) - 2 : n.keysLen(keySerializer) - 1;
    final int end = leaf ? 1 : 0;
    final int res = inclusive && leaf ? 1 : 0;
    //$DELAY$
    for (int i = start; i >= end; i--) {
      //$DELAY$
      final Object key2 = n.key(keySerializer, i);
      int comp = (key2 == null) ? -1 : keySerializer.comparator().compare(key2, key);
      if (comp < res) {
        if (leaf) {
          //$DELAY$
          return key2 == null ? null :
                  new Fun.Pair(i, n);
        } else {
          final long recid = n.child(i);
          if (recid == 0)
            continue;
          BNode n2 = engine.get(recid, nodeSerializer);
          if (n2.isLeaf()) {
            //check if first value is acceptable
            if (n2.keysLen(keySerializer) > 2 &&
                    keySerializer.comparator().compare(
                            n2.key(keySerializer, 1), key) >= (inclusive ? 1 : 0)) {
              continue;
            }
          }

          //$DELAY$
          return findSmallerNodeRecur(n2, key, inclusive);
        }
      }
    }

    return null;
  }


  @Override
  public Map.Entry<K, V> lastEntry() {
    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    BNode n = engine.get(rootRecid, nodeSerializer);
    //$DELAY$
    Entry e = lastEntryRecur(n);
    if (e != null && e.getValue() == null) return null;
    return e;
  }


  private Map.Entry<K, V> lastEntryRecur(BNode n) {
    if (n.isLeaf()) {
      //follow next node if available
      if (n.next() != 0) {
        BNode n2 = engine.get(n.next(), nodeSerializer);
        Map.Entry<K, V> ret = lastEntryRecur(n2);
        //$DELAY$
        if (ret != null)
          return ret;
      }

      //iterate over keys to find last non null key
      for (int i = n.keysLen(keySerializer) - 2; i > 0; i--) {
        Object k = n.key(keySerializer, i);
        if (k != null && n.valSize(valueSerializer) > 0) {
          Object val = valExpand(n.val(i - 1, valueSerializer));
          //$DELAY$
          if (val != null) {
            //$DELAY$
            return makeEntry(k, val);
          }
        }
      }
    } else {
      //dir node, dive deeper
      for (int i = n.childArrayLength() - 1; i >= 0; i--) {
        long childRecid = n.child(i);
        if (childRecid == 0) continue;
        BNode n2 = engine.get(childRecid, nodeSerializer);
        //$DELAY$
        Entry<K, V> ret = lastEntryRecur(n2);
        //$DELAY$
        if (ret != null)
          return ret;
      }
    }
    return null;
  }

  @Override
  public Map.Entry<K, V> lowerEntry(K key) {
    if (key == null) throw new NullPointerException();
    return findSmaller(key, false);
  }

  @Override
  public K lowerKey(K key) {
    Entry<K, V> n = lowerEntry(key);
    return (n == null) ? null : n.getKey();
  }

  @Override
  public Map.Entry<K, V> floorEntry(K key) {
    if (key == null) throw new NullPointerException();
    return findSmaller(key, true);
  }

  @Override
  public K floorKey(K key) {
    Entry<K, V> n = floorEntry(key);
    return (n == null) ? null : n.getKey();
  }

  @Override
  public Map.Entry<K, V> ceilingEntry(K key) {
    if (key == null) throw new NullPointerException();
    return findLarger(key, true);
  }

  protected Entry<K, V> findLarger(final K key, boolean inclusive) {
    if (key == null) return null;

    long current = engine.get(rootRecidRef, Serializer.RECID);

    BNode A = engine.get(current, nodeSerializer);

    //dive until  leaf
    //$DELAY$
    while (!A.isLeaf()) {
      current = nextDir((DirNode) A, key);
      //$DELAY$
      A = engine.get(current, nodeSerializer);
    }

    //now at leaf level
    LeafNode leaf = (LeafNode) A;
    //follow link until first matching node is found
    final int comp = inclusive ? 1 : 0;
    //$DELAY$
    while (true) {
      //$DELAY$
      for (int i = 1; i < leaf.keysLen(keySerializer) - 1; i++) {
        //$DELAY$
        if (leaf.key(keySerializer, i) == null) continue;
        //$DELAY$
        if (-leaf.compare(keySerializer, i, key) < comp) {
          //$DELAY$
          return makeEntry(leaf.key(keySerializer, i), valExpand(leaf.val(i - 1, valueSerializer)));
        }


      }
      if (leaf.next == 0) return null; //reached end
      //$DELAY$
      leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
    }

  }

  protected Fun.Pair<Integer, LeafNode> findLargerNode(final K key, boolean inclusive) {
    if (key == null) return null;

    long current = engine.get(rootRecidRef, Serializer.RECID);
    //$DELAY$
    BNode A = engine.get(current, nodeSerializer);

    //dive until  leaf
    while (!A.isLeaf()) {
      current = nextDir((DirNode) A, key);
      A = engine.get(current, nodeSerializer);
    }

    //now at leaf level
    LeafNode leaf = (LeafNode) A;
    //follow link until first matching node is found
    final int comp = inclusive ? 1 : 0;
    while (true) {
      //$DELAY$
      for (int i = 1; i < leaf.keysLen(keySerializer) - 1; i++) {
        if (leaf.key(keySerializer, i) == null) continue;
        //$DELAY$
        if (-leaf.compare(keySerializer, i, key) < comp) {
          return new Fun.Pair(i, leaf);
        }
      }
      if (leaf.next == 0) return null; //reached end
      leaf = (LeafNode) engine.get(leaf.next, nodeSerializer);
    }

  }


  @Override
  public K ceilingKey(K key) {
    if (key == null) throw new NullPointerException();
    Entry<K, V> n = ceilingEntry(key);
    return (n == null) ? null : n.getKey();
  }

  @Override
  public Map.Entry<K, V> higherEntry(K key) {
    if (key == null) throw new NullPointerException();
    return findLarger(key, false);
  }

  @Override
  public K higherKey(K key) {
    if (key == null) throw new NullPointerException();
    Entry<K, V> n = higherEntry(key);
    return (n == null) ? null : n.getKey();
  }

  @Override
  public boolean containsKey(Object key) {
    if (key == null) throw new NullPointerException();
    return get(key, false) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    if (value == null) throw new NullPointerException();
    Iterator<V> valueIter = valueIterator();
    //$DELAY$
    while (valueIter.hasNext()) {
      //$DELAY$
      if (valueSerializer.equals((V) value, valueIter.next()))
        return true;
    }
    return false;
  }


  @Override
  public K lastKey() {
    Entry<K, V> e = lastEntry();
    if (e == null) throw new NoSuchElementException();
    return e.getKey();
  }


  @Override
  public ConcurrentNavigableMap<K, V> subMap(K fromKey,
                                             boolean fromInclusive,
                                             K toKey,
                                             boolean toInclusive) {
    if (fromKey == null || toKey == null)
      throw new NullPointerException();
    return new SubMap<K, V>
            (this, fromKey, fromInclusive, toKey, toInclusive);
  }

  @Override
  public ConcurrentNavigableMap<K, V> headMap(K toKey,
                                              boolean inclusive) {
    if (toKey == null)
      throw new NullPointerException();
    return new SubMap<K, V>
            (this, null, false, toKey, inclusive);
  }

  @Override
  public ConcurrentNavigableMap<K, V> tailMap(K fromKey,
                                              boolean inclusive) {
    if (fromKey == null)
      throw new NullPointerException();
    return new SubMap<K, V>
            (this, fromKey, inclusive, null, false);
  }

  @Override
  public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  @Override
  public ConcurrentNavigableMap<K, V> headMap(K toKey) {
    return headMap(toKey, false);
  }

  @Override
  public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }


  Iterator<K> keyIterator() {
    return new BTreeKeyIterator(this);
  }

  Iterator<V> valueIterator() {
    return new BTreeValueIterator(this, null, false, null, false);
  }

  Iterator<Map.Entry<K, V>> entryIterator() {
    return new BTreeEntryIterator(this);
  }


    /* ---------------- View methods -------------- */

  @Override
  public NavigableSet<K> keySet() {
    return keySet;
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    return keySet;
  }

  @Override
  public Collection<V> values() {
    return values;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    return entrySet;
  }

  @Override
  public ConcurrentNavigableMap<K, V> descendingMap() {
    return descendingMap;
  }

  @Override
  public NavigableSet<K> descendingKeySet() {
    return descendingMap.keySet();
  }

  static <E> List<E> toList(Collection<E> c) {
    // Using size() here would be a pessimization.
    List<E> list = new ArrayList<E>();
    for (E e : c) {
      list.add(e);
    }
    return list;
  }


  static final class KeySet<E>
          extends AbstractSet<E>
          implements NavigableSet<E>,
          Closeable {

    protected final ConcurrentNavigableMap<E, Object> m;
    private final boolean hasValues;

    KeySet(ConcurrentNavigableMap<E, Object> map, boolean hasValues) {
      m = map;
      this.hasValues = hasValues;
    }

    @Override
    public int size() {
      return m.size();
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
      return m.containsKey(o);
    }

    @Override
    public boolean remove(Object o) {
      return m.remove(o) != null;
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public E lower(E e) {
      return m.lowerKey(e);
    }

    @Override
    public E floor(E e) {
      return m.floorKey(e);
    }

    @Override
    public E ceiling(E e) {
      return m.ceilingKey(e);
    }

    @Override
    public E higher(E e) {
      return m.higherKey(e);
    }

    @Override
    public Comparator<? super E> comparator() {
      return m.comparator();
    }

    @Override
    public E first() {
      return m.firstKey();
    }

    @Override
    public E last() {
      return m.lastKey();
    }

    @Override
    public E pollFirst() {
      Map.Entry<E, Object> e = m.pollFirstEntry();
      return e == null ? null : e.getKey();
    }

    @Override
    public E pollLast() {
      Map.Entry<E, Object> e = m.pollLastEntry();
      return e == null ? null : e.getKey();
    }

    @Override
    public Iterator<E> iterator() {
      if (m instanceof BTreeMap)
        return ((BTreeMap<E, Object>) m).keyIterator();
      else if (m instanceof SubMap)
        return ((BTreeMap.SubMap<E, Object>) m).keyIterator();
      else
        return ((BTreeMap.DescendingMap<E, Object>) m).keyIterator();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Set))
        return false;
      Collection<?> c = (Collection<?>) o;
      try {
        return containsAll(c) && c.containsAll(this);
      } catch (ClassCastException unused) {
        return false;
      } catch (NullPointerException unused) {
        return false;
      }
    }

    @Override
    public Object[] toArray() {
      return toList(this).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }

    @Override
    public Iterator<E> descendingIterator() {
      return descendingSet().iterator();
    }

    @Override
    public NavigableSet<E> subSet(E fromElement,
                                  boolean fromInclusive,
                                  E toElement,
                                  boolean toInclusive) {
      return new KeySet<E>(m.subMap(fromElement, fromInclusive,
              toElement, toInclusive), hasValues);
    }

    @Override
    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
      return new KeySet<E>(m.headMap(toElement, inclusive), hasValues);
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
      return new KeySet<E>(m.tailMap(fromElement, inclusive), hasValues);
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, E toElement) {
      return subSet(fromElement, true, toElement, false);
    }

    @Override
    public NavigableSet<E> headSet(E toElement) {
      return headSet(toElement, false);
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement) {
      return tailSet(fromElement, true);
    }

    @Override
    public NavigableSet<E> descendingSet() {
      return new KeySet(m.descendingMap(), hasValues);
    }

    @Override
    public boolean add(E k) {
      if (hasValues)
        throw new UnsupportedOperationException();
      else
        return m.put(k, Boolean.TRUE) == null;
    }

    @Override
    public void close() {
      if (m instanceof BTreeMap)
        ((BTreeMap) m).close();
    }
  }

  static final class Values<E> extends AbstractCollection<E> {
    private final ConcurrentNavigableMap<Object, E> m;

    Values(ConcurrentNavigableMap<Object, E> map) {
      m = map;
    }

    @Override
    public Iterator<E> iterator() {
      if (m instanceof BTreeMap)
        return ((BTreeMap<Object, E>) m).valueIterator();
      else
        return ((SubMap<Object, E>) m).valueIterator();
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public int size() {
      return m.size();
    }

    @Override
    public boolean contains(Object o) {
      return m.containsValue(o);
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public Object[] toArray() {
      return toList(this).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
  }

  static final class EntrySet<K1, V1> extends AbstractSet<Map.Entry<K1, V1>> {
    private final ConcurrentNavigableMap<K1, V1> m;
    private final Serializer valueSerializer;

    EntrySet(ConcurrentNavigableMap<K1, V1> map, Serializer valueSerializer) {
      m = map;
      this.valueSerializer = valueSerializer;
    }

    @Override
    public Iterator<Map.Entry<K1, V1>> iterator() {
      if (m instanceof BTreeMap)
        return ((BTreeMap<K1, V1>) m).entryIterator();
      else if (m instanceof SubMap)
        return ((SubMap<K1, V1>) m).entryIterator();
      else
        return ((DescendingMap<K1, V1>) m).entryIterator();
    }

    @Override
    public boolean contains(Object o) {
      if (!(o instanceof Map.Entry))
        return false;
      Map.Entry<K1, V1> e = (Map.Entry<K1, V1>) o;
      K1 key = e.getKey();
      if (key == null) return false;
      V1 v = m.get(key);
      //$DELAY$
      return v != null && valueSerializer.equals(v, e.getValue());
    }

    @Override
    public boolean remove(Object o) {
      if (!(o instanceof Map.Entry))
        return false;
      Map.Entry<K1, V1> e = (Map.Entry<K1, V1>) o;
      K1 key = e.getKey();
      if (key == null) return false;
      return m.remove(key,
              e.getValue());
    }

    @Override
    public boolean isEmpty() {
      return m.isEmpty();
    }

    @Override
    public int size() {
      return m.size();
    }

    @Override
    public void clear() {
      m.clear();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;
      if (!(o instanceof Set))
        return false;
      Collection<?> c = (Collection<?>) o;
      try {
        return containsAll(c) && c.containsAll(this);
      } catch (ClassCastException unused) {
        return false;
      } catch (NullPointerException unused) {
        return false;
      }
    }

    @Override
    public Object[] toArray() {
      return toList(this).toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
      return toList(this).toArray(a);
    }
  }


  static protected class SubMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {

    protected final BTreeMap<K, V> m;

    protected final K lo;
    protected final boolean loInclusive;

    protected final K hi;
    protected final boolean hiInclusive;

    public SubMap(BTreeMap<K, V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive) {
      this.m = m;
      this.lo = lo;
      this.loInclusive = loInclusive;
      this.hi = hi;
      this.hiInclusive = hiInclusive;
      if (lo != null && hi != null && m.keySerializer.comparator().compare(lo, hi) > 0) {
        throw new IllegalArgumentException();
      }


    }


/* ----------------  Map API methods -------------- */

    @Override
    public boolean containsKey(Object key) {
      if (key == null) throw new NullPointerException();
      K k = (K) key;
      return inBounds(k) && m.containsKey(k);
    }

    @Override
    public V get(Object key) {
      if (key == null) throw new NullPointerException();
      K k = (K) key;
      return ((!inBounds(k)) ? null : m.get(k));
    }

    @Override
    public V put(K key, V value) {
      checkKeyBounds(key);
      return m.put(key, value);
    }

    @Override
    public V remove(Object key) {
      if (key == null)
        throw new NullPointerException("key null");
      K k = (K) key;
      return (!inBounds(k)) ? null : m.remove(k);
    }

    @Override
    public int size() {
      //TODO use counted btrees once they become available
      if (hi == null && lo == null)
        return m.size();

      Iterator<K> i = keyIterator();
      long counter = 0;
      while (i.hasNext()) {
        counter++;
        i.next();
      }
      return (int) Math.min(counter, Integer.MAX_VALUE);
    }

    @Override
    public boolean isEmpty() {
      return !keyIterator().hasNext();
    }

    @Override
    public boolean containsValue(Object value) {
      if (value == null) throw new NullPointerException();
      Iterator<V> i = valueIterator();
      while (i.hasNext()) {
        if (m.valueSerializer.equals((V) value, i.next()))
          return true;
      }
      return false;
    }

    @Override
    public void clear() {
      Iterator<K> i = keyIterator();
      while (i.hasNext()) {
        i.next();
        i.remove();
      }
    }


        /* ----------------  ConcurrentMap API methods -------------- */

    @Override
    public V putIfAbsent(K key, V value) {
      checkKeyBounds(key);
      return m.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
      K k = (K) key;
      return inBounds(k) && m.remove(k, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
      checkKeyBounds(key);
      return m.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
      checkKeyBounds(key);
      return m.replace(key, value);
    }

        /* ----------------  SortedMap API methods -------------- */

    @Override
    public Comparator<? super K> comparator() {
      return m.comparator();
    }

        /* ----------------  Relational methods -------------- */

    @Override
    public Map.Entry<K, V> lowerEntry(K key) {
      if (key == null) throw new NullPointerException();
      if (tooLow(key)) return null;

      if (tooHigh(key))
        return lastEntry();

      Entry<K, V> r = m.lowerEntry(key);
      return r != null && !tooLow(r.getKey()) ? r : null;
    }

    @Override
    public K lowerKey(K key) {
      Entry<K, V> n = lowerEntry(key);
      return (n == null) ? null : n.getKey();
    }

    @Override
    public Map.Entry<K, V> floorEntry(K key) {
      if (key == null) throw new NullPointerException();
      if (tooLow(key)) return null;

      if (tooHigh(key)) {
        return lastEntry();
      }

      Entry<K, V> ret = m.floorEntry(key);
      if (ret != null && tooLow(ret.getKey())) return null;
      return ret;

    }

    @Override
    public K floorKey(K key) {
      Entry<K, V> n = floorEntry(key);
      return (n == null) ? null : n.getKey();
    }

    @Override
    public Map.Entry<K, V> ceilingEntry(K key) {
      if (key == null) throw new NullPointerException();
      if (tooHigh(key)) return null;

      if (tooLow(key)) {
        return firstEntry();
      }

      Entry<K, V> ret = m.ceilingEntry(key);
      if (ret != null && tooHigh(ret.getKey())) return null;
      return ret;
    }

    @Override
    public K ceilingKey(K key) {
      Entry<K, V> k = ceilingEntry(key);
      return k != null ? k.getKey() : null;
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
      Entry<K, V> r = m.higherEntry(key);
      return r != null && inBounds(r.getKey()) ? r : null;
    }

    @Override
    public K higherKey(K key) {
      Entry<K, V> k = higherEntry(key);
      return k != null ? k.getKey() : null;
    }


    @Override
    public K firstKey() {
      Entry<K, V> e = firstEntry();
      if (e == null) throw new NoSuchElementException();
      return e.getKey();
    }

    @Override
    public K lastKey() {
      Entry<K, V> e = lastEntry();
      if (e == null) throw new NoSuchElementException();
      return e.getKey();
    }


    @Override
    public Map.Entry<K, V> firstEntry() {
      Entry<K, V> k =
              lo == null ?
                      m.firstEntry() :
                      m.findLarger(lo, loInclusive);
      return k != null && inBounds(k.getKey()) ? k : null;

    }

    @Override
    public Map.Entry<K, V> lastEntry() {
      Entry<K, V> k =
              hi == null ?
                      m.lastEntry() :
                      m.findSmaller(hi, hiInclusive);

      return k != null && inBounds(k.getKey()) ? k : null;
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
      while (true) {
        Entry<K, V> e = firstEntry();
        if (e == null || remove(e.getKey(), e.getValue())) {
          return e;
        }
      }
    }

    @Override
    public Entry<K, V> pollLastEntry() {
      while (true) {
        Entry<K, V> e = lastEntry();
        if (e == null || remove(e.getKey(), e.getValue())) {
          return e;
        }
      }
    }


    /**
     * Utility to create submaps, where given bounds override
     * unbounded(null) ones and/or are checked against bounded ones.
     */
    private SubMap<K, V> newSubMap(K fromKey,
                                   boolean fromInclusive,
                                   K toKey,
                                   boolean toInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

      if (lo != null) {
        if (fromKey == null) {
          fromKey = lo;
          fromInclusive = loInclusive;
        } else {
          int c = m.keySerializer.comparator().compare(fromKey, lo);
          if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
            throw new IllegalArgumentException("key out of range");
        }
      }
      if (hi != null) {
        if (toKey == null) {
          toKey = hi;
          toInclusive = hiInclusive;
        } else {
          int c = m.keySerializer.comparator().compare(toKey, hi);
          if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
            throw new IllegalArgumentException("key out of range");
        }
      }
      return new SubMap<K, V>(m, fromKey, fromInclusive,
              toKey, toInclusive);
    }

    @Override
    public SubMap<K, V> subMap(K fromKey,
                               boolean fromInclusive,
                               K toKey,
                               boolean toInclusive) {
      if (fromKey == null || toKey == null)
        throw new NullPointerException();
      return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public SubMap<K, V> headMap(K toKey,
                                boolean inclusive) {
      if (toKey == null)
        throw new NullPointerException();
      return newSubMap(null, false, toKey, inclusive);
    }

    @Override
    public SubMap<K, V> tailMap(K fromKey,
                                boolean inclusive) {
      if (fromKey == null)
        throw new NullPointerException();
      return newSubMap(fromKey, inclusive, null, false);
    }

    @Override
    public SubMap<K, V> subMap(K fromKey, K toKey) {
      return subMap(fromKey, true, toKey, false);
    }

    @Override
    public SubMap<K, V> headMap(K toKey) {
      return headMap(toKey, false);
    }

    @Override
    public SubMap<K, V> tailMap(K fromKey) {
      return tailMap(fromKey, true);
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
      return new DescendingMap(m, lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
      return new KeySet<K>((ConcurrentNavigableMap<K, Object>) this, m.hasValues);
    }


        /* ----------------  Utilities -------------- */


    private boolean tooLow(K key) {
      if (lo != null) {
        int c = m.keySerializer.comparator().compare(key, lo);
        if (c < 0 || (c == 0 && !loInclusive))
          return true;
      }
      return false;
    }

    private boolean tooHigh(K key) {
      if (hi != null) {
        int c = m.keySerializer.comparator().compare(key, hi);
        if (c > 0 || (c == 0 && !hiInclusive))
          return true;
      }
      return false;
    }

    private boolean inBounds(K key) {
      return !tooLow(key) && !tooHigh(key);
    }

    private void checkKeyBounds(K key) throws IllegalArgumentException {
      if (key == null)
        throw new NullPointerException();
      if (!inBounds(key))
        throw new IllegalArgumentException("key out of range");
    }


    @Override
    public NavigableSet<K> keySet() {
      return new KeySet<K>((ConcurrentNavigableMap<K, Object>) this, m.hasValues);
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
      return new DescendingMap<K, V>(m, lo, loInclusive, hi, hiInclusive).keySet();
    }


    @Override
    public Set<Entry<K, V>> entrySet() {
      return new EntrySet<K, V>(this, m.valueSerializer);
    }


    Iterator<K> keyIterator() {
      return new BTreeKeyIterator(m, lo, loInclusive, hi, hiInclusive);
    }

    Iterator<V> valueIterator() {
      return new BTreeValueIterator(m, lo, loInclusive, hi, hiInclusive);
    }

    Iterator<Map.Entry<K, V>> entryIterator() {
      return new BTreeEntryIterator(m, lo, loInclusive, hi, hiInclusive);
    }

  }


  static protected class DescendingMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {

    protected final BTreeMap<K, V> m;

    protected final K lo;
    protected final boolean loInclusive;

    protected final K hi;
    protected final boolean hiInclusive;

    public DescendingMap(BTreeMap<K, V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive) {
      this.m = m;
      this.lo = lo;
      this.loInclusive = loInclusive;
      this.hi = hi;
      this.hiInclusive = hiInclusive;
      if (lo != null && hi != null && m.keySerializer.comparator().compare(lo, hi) > 0) {
        throw new IllegalArgumentException();
      }


    }


/* ----------------  Map API methods -------------- */

    @Override
    public boolean containsKey(Object key) {
      if (key == null) throw new NullPointerException();
      K k = (K) key;
      return inBounds(k) && m.containsKey(k);
    }

    @Override
    public V get(Object key) {
      if (key == null) throw new NullPointerException();
      K k = (K) key;
      return ((!inBounds(k)) ? null : m.get(k));
    }

    @Override
    public V put(K key, V value) {
      checkKeyBounds(key);
      return m.put(key, value);
    }

    @Override
    public V remove(Object key) {
      K k = (K) key;
      return (!inBounds(k)) ? null : m.remove(k);
    }

    @Override
    public int size() {
      if (hi == null && lo == null)
        return m.size();

      Iterator<K> i = keyIterator();
      long counter = 0;
      while (i.hasNext()) {
        counter++;
        i.next();
      }
      return (int) Math.min(counter, Integer.MAX_VALUE);
    }

    @Override
    public boolean isEmpty() {
      return !keyIterator().hasNext();
    }

    @Override
    public boolean containsValue(Object value) {
      if (value == null) throw new NullPointerException();
      Iterator<V> i = valueIterator();
      while (i.hasNext()) {
        if (m.valueSerializer.equals((V) value, i.next()))
          return true;
      }
      return false;
    }

    @Override
    public void clear() {
      Iterator<K> i = keyIterator();
      while (i.hasNext()) {
        i.next();
        i.remove();
      }
    }


        /* ----------------  ConcurrentMap API methods -------------- */

    @Override
    public V putIfAbsent(K key, V value) {
      checkKeyBounds(key);
      return m.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(Object key, Object value) {
      K k = (K) key;
      return inBounds(k) && m.remove(k, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
      checkKeyBounds(key);
      return m.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
      checkKeyBounds(key);
      return m.replace(key, value);
    }

        /* ----------------  SortedMap API methods -------------- */

    @Override
    public Comparator<? super K> comparator() {
      return m.comparator();
    }

        /* ----------------  Relational methods -------------- */

    @Override
    public Map.Entry<K, V> higherEntry(K key) {
      if (key == null) throw new NullPointerException();
      if (tooLow(key)) return null;

      if (tooHigh(key))
        return firstEntry();

      Entry<K, V> r = m.lowerEntry(key);
      return r != null && !tooLow(r.getKey()) ? r : null;
    }

    @Override
    public K lowerKey(K key) {
      Entry<K, V> n = lowerEntry(key);
      return (n == null) ? null : n.getKey();
    }

    @Override
    public Map.Entry<K, V> ceilingEntry(K key) {
      if (key == null) throw new NullPointerException();
      if (tooLow(key)) return null;

      if (tooHigh(key)) {
        return firstEntry();
      }

      Entry<K, V> ret = m.floorEntry(key);
      if (ret != null && tooLow(ret.getKey())) return null;
      return ret;

    }

    @Override
    public K floorKey(K key) {
      Entry<K, V> n = floorEntry(key);
      return (n == null) ? null : n.getKey();
    }

    @Override
    public Map.Entry<K, V> floorEntry(K key) {
      if (key == null) throw new NullPointerException();
      if (tooHigh(key)) return null;

      if (tooLow(key)) {
        return lastEntry();
      }

      Entry<K, V> ret = m.ceilingEntry(key);
      if (ret != null && tooHigh(ret.getKey())) return null;
      return ret;
    }

    @Override
    public K ceilingKey(K key) {
      Entry<K, V> k = ceilingEntry(key);
      return k != null ? k.getKey() : null;
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
      Entry<K, V> r = m.higherEntry(key);
      return r != null && inBounds(r.getKey()) ? r : null;
    }

    @Override
    public K higherKey(K key) {
      Entry<K, V> k = higherEntry(key);
      return k != null ? k.getKey() : null;
    }


    @Override
    public K firstKey() {
      Entry<K, V> e = firstEntry();
      if (e == null) throw new NoSuchElementException();
      return e.getKey();
    }

    @Override
    public K lastKey() {
      Entry<K, V> e = lastEntry();
      if (e == null) throw new NoSuchElementException();
      return e.getKey();
    }


    @Override
    public Map.Entry<K, V> lastEntry() {
      Entry<K, V> k =
              lo == null ?
                      m.firstEntry() :
                      m.findLarger(lo, loInclusive);
      return k != null && inBounds(k.getKey()) ? k : null;

    }

    @Override
    public Map.Entry<K, V> firstEntry() {
      Entry<K, V> k =
              hi == null ?
                      m.lastEntry() :
                      m.findSmaller(hi, hiInclusive);

      return k != null && inBounds(k.getKey()) ? k : null;
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
      while (true) {
        Entry<K, V> e = firstEntry();
        if (e == null || remove(e.getKey(), e.getValue())) {
          return e;
        }
      }
    }

    @Override
    public Entry<K, V> pollLastEntry() {
      while (true) {
        Entry<K, V> e = lastEntry();
        if (e == null || remove(e.getKey(), e.getValue())) {
          return e;
        }
      }
    }


    /**
     * Utility to create submaps, where given bounds override
     * unbounded(null) ones and/or are checked against bounded ones.
     */
    private DescendingMap<K, V> newSubMap(
            K toKey,
            boolean toInclusive,
            K fromKey,
            boolean fromInclusive) {

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

      if (lo != null) {
        if (fromKey == null) {
          fromKey = lo;
          fromInclusive = loInclusive;
        } else {
          int c = m.keySerializer.comparator().compare(fromKey, lo);
          if (c < 0 || (c == 0 && !loInclusive && fromInclusive))
            throw new IllegalArgumentException("key out of range");
        }
      }
      if (hi != null) {
        if (toKey == null) {
          toKey = hi;
          toInclusive = hiInclusive;
        } else {
          int c = m.keySerializer.comparator().compare(toKey, hi);
          if (c > 0 || (c == 0 && !hiInclusive && toInclusive))
            throw new IllegalArgumentException("key out of range");
        }
      }
      return new DescendingMap<K, V>(m, fromKey, fromInclusive,
              toKey, toInclusive);
    }

    @Override
    public DescendingMap<K, V> subMap(K fromKey,
                                      boolean fromInclusive,
                                      K toKey,
                                      boolean toInclusive) {
      if (fromKey == null || toKey == null)
        throw new NullPointerException();
      return newSubMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public DescendingMap<K, V> headMap(K toKey,
                                       boolean inclusive) {
      if (toKey == null)
        throw new NullPointerException();
      return newSubMap(null, false, toKey, inclusive);
    }

    @Override
    public DescendingMap<K, V> tailMap(K fromKey,
                                       boolean inclusive) {
      if (fromKey == null)
        throw new NullPointerException();
      return newSubMap(fromKey, inclusive, null, false);
    }

    @Override
    public DescendingMap<K, V> subMap(K fromKey, K toKey) {
      return subMap(fromKey, true, toKey, false);
    }

    @Override
    public DescendingMap<K, V> headMap(K toKey) {
      return headMap(toKey, false);
    }

    @Override
    public DescendingMap<K, V> tailMap(K fromKey) {
      return tailMap(fromKey, true);
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap() {
      if (lo == null && hi == null) return m;
      return m.subMap(lo, loInclusive, hi, hiInclusive);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
      return new KeySet<K>((ConcurrentNavigableMap<K, Object>) this, m.hasValues);
    }


        /* ----------------  Utilities -------------- */


    private boolean tooLow(K key) {
      if (lo != null) {
        int c = m.keySerializer.comparator().compare(key, lo);
        if (c < 0 || (c == 0 && !loInclusive))
          return true;
      }
      return false;
    }

    private boolean tooHigh(K key) {
      if (hi != null) {
        int c = m.keySerializer.comparator().compare(key, hi);
        if (c > 0 || (c == 0 && !hiInclusive))
          return true;
      }
      return false;
    }

    private boolean inBounds(K key) {
      return !tooLow(key) && !tooHigh(key);
    }

    private void checkKeyBounds(K key) throws IllegalArgumentException {
      if (key == null)
        throw new NullPointerException();
      if (!inBounds(key))
        throw new IllegalArgumentException("key out of range");
    }


    @Override
    public NavigableSet<K> keySet() {
      return new KeySet<K>((ConcurrentNavigableMap<K, Object>) this, m.hasValues);
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
      return new KeySet<K>((ConcurrentNavigableMap<K, Object>) descendingMap(), m.hasValues);
    }


    @Override
    public Set<Entry<K, V>> entrySet() {
      return new EntrySet<K, V>(this, m.valueSerializer);
    }


        /*
         * ITERATORS
         */

    Iterator<K> keyIterator() {
      return new BTreeDescendingKeyIterator<K>(m, lo, loInclusive, hi, hiInclusive);
    }

    Iterator<V> valueIterator() {
      return new BTreeDescendingValueIterator<V>(m, lo, loInclusive, hi, hiInclusive);
    }

    Iterator<Map.Entry<K, V>> entryIterator() {
      return new BTreeDescendingEntryIterator<K, V>(m, lo, loInclusive, hi, hiInclusive);
    }

  }


  /**
   * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
   * Useful if you need consistent view on Map.
   * <p/>
   * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
   * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
   *
   * @return snapshot
   */
  public NavigableMap<K, V> snapshot() {
    Engine snapshot = TxEngine.createSnapshotFor(engine);

    return new BTreeMap<K, V>(
            snapshot,
            closeEngine,
            rootRecidRef,
            maxNodeSize,
            valsOutsideNodes,
            counter == null ? 0L : counter.recid,
            keySerializer,
            valueSerializer,
            numberOfNodeMetas
    );
  }


  protected final Object modListenersLock = new Object();
  protected Bind.MapListener<K, V>[] modListeners = new Bind.MapListener[0];

  @Override
  public void modificationListenerAdd(Bind.MapListener<K, V> listener) {
    synchronized (modListenersLock) {
      Bind.MapListener<K, V>[] modListeners2 =
              Arrays.copyOf(modListeners, modListeners.length + 1);
      modListeners2[modListeners2.length - 1] = listener;
      modListeners = modListeners2;
    }

  }

  @Override
  public void modificationListenerRemove(Bind.MapListener<K, V> listener) {
    synchronized (modListenersLock) {
      for (int i = 0; i < modListeners.length; i++) {
        if (modListeners[i] == listener) modListeners[i] = null;
      }
    }
  }

  //TODO check  references to notify
  protected void notify(K key, V oldValue, V newValue) {
    if (CC.ASSERT && !(!(oldValue instanceof ValRef)))
      throw new AssertionError();
    if (CC.ASSERT && !(!(newValue instanceof ValRef)))
      throw new AssertionError();

    Bind.MapListener<K, V>[] modListeners2 = modListeners;
    for (Bind.MapListener<K, V> listener : modListeners2) {
      if (listener != null)
        listener.update(key, oldValue, newValue);
    }
  }


  public Engine getEngine() {
    return engine;
  }


  public void printTreeStructure() {
    final long rootRecid = engine.get(rootRecidRef, Serializer.RECID);
    printRecur(this, rootRecid, "");
  }

  private static void printRecur(BTreeMap m, long recid, String s) {
    BTreeMap.BNode n = (BTreeMap.BNode) m.engine.get(recid, m.nodeSerializer);
    System.out.println(s + recid + "-" + n);
    if (!n.isLeaf()) {
      int childArrayLen = n.childArrayLength() - 1;
      for (int i = 0; i < childArrayLen; i++) {
        long recid2 = n.child(i);
        if (recid2 != 0)
          printRecur(m, recid2, s + "  ");
      }
    }
  }


  /**
   * expand array size by 1, and put value at given position. No items from original array are lost
   */
  protected static Object[] arrayPut(final Object[] array, final int pos, final Object value) {
    final Object[] ret = Arrays.copyOf(array, array.length + 1);
    if (pos < array.length) {
      System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
    }
    ret[pos] = value;
    return ret;
  }

  protected static void assertNoLocks(LongConcurrentHashMap<ReentrantLock> locks) {
    LongConcurrentHashMap.LongMapIterator<ReentrantLock> i = locks.longMapIterator();
    while (i.moveToNext()) {
      if (i.value().isHeldByCurrentThread()) {
        throw new AssertionError("Node " + i.key() + " is still locked");
      }
    }
  }


  protected static void unlock(LongConcurrentHashMap<ReentrantLock> locks, final long recid) {
    locks.get(recid).unlock();
    // System.out.println(Thread.currentThread() + " releases lock for rec " + recid + ", count: " +
      // locks.get(recid).getHoldCount());
    /*
    if (CC.ASSERT && !(t == Thread.currentThread()))
      throw new AssertionError("unlocked wrong thread");
      */
  }

  protected static void unlockAll(LongConcurrentHashMap<ReentrantLock> locks) {
    final Thread t = Thread.currentThread();
    LongConcurrentHashMap.LongMapIterator<ReentrantLock> iter = locks.longMapIterator();
    while (iter.moveToNext()) {
      ReentrantLock lock = iter.value();
      lock.unlock();
      if (lock.getHoldCount() != 0) {
        System.out.println(Thread.currentThread() + " acquired " + lock + " for " +
                (lock.getHoldCount() + 1) + " times, but released only once");
      }
    }
  }


  protected static void lock(LongConcurrentHashMap<ReentrantLock> locks,
                             long recid) throws InterruptedException {
    //feel free to rewrite, if you know better (more efficient) way

    /*
    //check node is not already locked by this thread
    if (CC.ASSERT && !(locks.get(recid) != currentThread))
      throw new AssertionError("node already locked by current thread: " + recid);
    */
    ReentrantLock newLock = new ReentrantLock();
    ReentrantLock currentNodeLock = locks.putIfAbsent(recid, newLock);
    if (currentNodeLock == null) {
      //System.out.println("add new lock " + newLock + " for record " + recid);
      newLock.lock();
    } else {
      currentNodeLock.lock();
    }
    // System.out.println(Thread.currentThread() + " acquires lock for rec " + recid + " count:" +
       //      count);
  }


  public void checkStructure() {
    Store.LongObjectMap recids = new Store.LongObjectMap();
    final long recid = engine.get(rootRecidRef, Serializer.RECID);

    checkNodeRecur(recid, recids);

  }

  private void checkNodeRecur(long rootRecid, Store.LongObjectMap recids) {
    BNode n = engine.get(rootRecid, nodeSerializer);
    n.checkStructure(keySerializer, valueSerializer);

    if (recids.get(rootRecid) != null) {
      throw new DBException.DataCorruption("Duplicate recid: " + rootRecid);
    }
    recids.put(rootRecid, this);

    if (n.next() != 0L && recids.get(n.next()) == null) {
      throw new DBException.DataCorruption("Next link was not found: " + n);
    }
    if (n.next() == rootRecid) {
      throw new DBException.DataCorruption("Recursive next: " + n);
    }
    if (!n.isLeaf()) {
      for (int i = n.childArrayLength() - 1; i >= 0; i--) {
        long recid = n.child(i);
        if (recid == rootRecid) {
          throw new DBException.DataCorruption("Recursive recid: " + n);
        }

        if (recid == 0 || recid == n.next()) {
          continue;
        }
        checkNodeRecur(recid, recids);

      }
    }

  }

  @Override
  public void close() {
    if (closeEngine) {
      engine.close();
    }
  }


}
