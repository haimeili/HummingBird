package cpslab.db;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

@SuppressWarnings({"unchecked", "rawtypes"})
public class PartitionedHTreeMapTest {
  static DummyPartitioner partitioner = new DummyPartitioner(1);

  static Path tempDirFile;

  @BeforeClass
  public static void onlyOnce() {
    try {
      tempDirFile = Files.createTempDirectory(String.valueOf(System.currentTimeMillis()));
      //PartitionedHTreeMap.updateDirectoryNodeSize(128, 32);
      // PartitionedHTreeMap.updateBucketLength(28);
      System.out.println("----------------------------------------");
      System.out.println(tempDirFile.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDirSerializer() throws IOException {
    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "test_simple_put",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    Object dir = new int[4];

    for (int slot = 1; slot < 127; slot += 1 + slot / 5) {
      dir = m.putNewRecordIdInDir(dir, slot, slot * 1111);
    }

    DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
    m.DIR_SERIALIZER.serialize(out, dir);

    DataIO.DataInputByteBuffer in = swap(out);

    int[] dir2 = (int[]) m.DIR_SERIALIZER.deserialize(in, -1);
    org.junit.Assert.assertArrayEquals((int[]) dir, dir2);

    for (int slot = 1; slot < 127; slot += 1 + slot / 5) {
      int offset = m.dirOffsetFromSlot(dir2, slot);
      org.junit.Assert.assertEquals(slot * 1111, PartitionedHTreeMap.dirGet(dir2, offset));
    }
  }

  DataIO.DataInputByteBuffer swap(DataIO.DataOutputByteArray d) {
    byte[] b = d.copyBytes();
    return new DataIO.DataInputByteBuffer(ByteBuffer.wrap(b), 0);
  }


  @Test
  public void ln_serialization() throws IOException {

    PartitionedHTreeMap.LinkedNode n = new PartitionedHTreeMap.LinkedNode(123456, 123L, 456L);

    DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "ln_serialization",
            partitioner,
            false,
            2,
            Serializer.BASIC,
            Serializer.BASIC,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    m.LN_SERIALIZER.serialize(out, n);

    DataIO.DataInputByteBuffer in = swap(out);

    PartitionedHTreeMap.LinkedNode n2 =
            (PartitionedHTreeMap.LinkedNode) m.LN_SERIALIZER.deserialize(in, -1);

    org.junit.Assert.assertEquals(123456, n2.next);
    org.junit.Assert.assertEquals(123L, n2.key);
    org.junit.Assert.assertEquals(456L, n2.value);
  }

  @Test
  public void test_simple_put() {

    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "test_simple_put",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    m.put(111, 222);
    m.put(333, 444);
    org.junit.Assert.assertTrue(m.containsKey(111));
    org.junit.Assert.assertTrue(!m.containsKey(222));
    org.junit.Assert.assertTrue(m.containsKey(333));
    org.junit.Assert.assertTrue(!m.containsKey(444));

    org.junit.Assert.assertEquals(222, m.get(111));
    org.junit.Assert.assertEquals(null, m.get(222));
    org.junit.Assert.assertEquals(444, m.get(333));
  }

  @Test
  public void test_hash_collision() {

    PartitionedHTreeMap m = new PartitionedHTreeMap(0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.LONG,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    for (int i = 0; i < 20; i++) {
      m.put(i, (long) (i + 100));
    }

    for (int i = 0; i < 20; i++) {
      org.junit.Assert.assertTrue(m.containsKey(i));
      org.junit.Assert.assertEquals((long) (i + 100), m.get(i));
    }

    m.put(11, 1111L);
    org.junit.Assert.assertEquals(1111L, m.get(11));
  }

  @Test
  public void test_hash_dir_expand() {
    PartitionedHTreeMap m = new PartitionedHTreeMap( 0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE) {
      @Override
      public int hash(Object key) {
        return 0;
      }
    };
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    for (int i = 0; i < m.BUCKET_OVERFLOW; i++) {
      m.put(i, i);
    }

    Engine engine = (Engine) m.engines.get(0);

    //segment should not be expanded
    int[] l = (int[]) engine.get(((Long[]) m.partitionRootRec.get(0))[0],
            m.DIR_SERIALIZER);
    org.junit.Assert.assertEquals(4 + 1, l.length);
    long recid = l[4];
    org.junit.Assert.assertEquals(1, recid & 1);  //last bite indicates leaf
    org.junit.Assert.assertEquals(1, l[0]);
    //all others should be null
    for (int i = 1; i < 4; i++)
      org.junit.Assert.assertEquals(0, l[i]);

    recid = recid >>> 1;

    for (int i = m.BUCKET_OVERFLOW - 1; i >= 0; i--) {
      org.junit.Assert.assertTrue(recid != 0);
      PartitionedHTreeMap.LinkedNode n =
              (PartitionedHTreeMap.LinkedNode) engine.get(recid, m.LN_SERIALIZER);
      org.junit.Assert.assertEquals(i, n.key);
      org.junit.Assert.assertEquals(i, n.value);
      recid = n.next;
    }

    //adding one more item should trigger dir expansion to next level
    m.put(m.BUCKET_OVERFLOW, m.BUCKET_OVERFLOW);

    recid = ((Long[]) m.partitionRootRec.get(0))[0];

    l = (int[]) engine.get(recid, m.DIR_SERIALIZER);
    org.junit.Assert.assertEquals(4 + 1, l.length);
    recid = l[4];
    org.junit.Assert.assertEquals(0, recid & 1);  //last bite indicates leaf
    org.junit.Assert.assertEquals(1, l[0]);

    //all others should be null
    for (int i = 1; i < 4; i++)
      org.junit.Assert.assertEquals(0, l[i]);

    recid = recid >>> 1;

    l = (int[]) engine.get(recid, m.DIR_SERIALIZER);

    org.junit.Assert.assertEquals(4 + 1, l.length);
    recid = l[4];
    org.junit.Assert.assertEquals(1, recid & 1);  //last bite indicates leaf
    org.junit.Assert.assertEquals(1, l[0]);

    //all others should be null
    for (int i = 1; i < 4; i++)
      org.junit.Assert.assertEquals(0, l[i]);

    recid = recid >>> 1;


    for (int i = 0; i <= m.BUCKET_OVERFLOW; i++) {
      org.junit.Assert.assertTrue(recid != 0);
      PartitionedHTreeMap.LinkedNode n =
              (PartitionedHTreeMap.LinkedNode) engine.get(recid, m.LN_SERIALIZER);

      org.junit.Assert.assertNotNull(n);
      org.junit.Assert.assertEquals(i, n.key);
      org.junit.Assert.assertEquals(i, n.value);
      recid = n.next;
    }
  }

  @Test
  public void test_delete() {
    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE) {
      @Override
      public int hash(Object key) {
        return 0;
      }
    };

    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    for (int i = 0; i < 20; i++) {
      m.put(i, i + 100);
    }

    for (int i = 0; i < 20; i++) {
      org.junit.Assert.assertTrue(m.containsKey(i));
      org.junit.Assert.assertEquals(i + 100, m.get(i));
    }


    for (int i = 0; i < 20; i++) {
      m.remove(i);
    }

    for (int i = 0; i < 20; i++) {
      org.junit.Assert.assertTrue(!m.containsKey(i));
      org.junit.Assert.assertEquals(null, m.get(i));
    }
  }

  @Test
  public void clear() {
    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            Serializer.INTEGER,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    for (int i = 0; i < 100; i++) {
      m.put(i, i);
    }
    m.clear();
    org.junit.Assert.assertTrue(m.isEmpty());
    org.junit.Assert.assertEquals(0, m.size());
  }

  @Ignore //(timeout = 10000)
  public void testIteration() {
    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.BASIC,
            Serializer.BASIC,
            null,
            null,
            false,
            Long.MAX_VALUE) {

      @Override
      public int hash(Object key) {
        return (Integer) key;
      }
    };
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    final int max = 140;
    final int inc = 111111;

    for (Integer i = 0; i < max; i++) {
      m.put(i, i + inc);
    }

    Iterator<Integer> keys = m.keySet(0).iterator();
    for (Integer i = 0; i < max; i++) {
      org.junit.Assert.assertTrue(keys.hasNext());
      org.junit.Assert.assertEquals(i, keys.next());
    }
    org.junit.Assert.assertTrue(!keys.hasNext());

    Iterator<Integer> vals = m.values(0).iterator();
    for (Integer i = inc; i < max + inc; i++) {
      org.junit.Assert. assertTrue(vals.hasNext());
      org.junit.Assert. assertEquals(i, vals.next());
    }
    org.junit.Assert.assertTrue(!vals.hasNext());


    //great it worked, test stuff spread across segments
    m.clear();
    org.junit.Assert.assertTrue(m.isEmpty());

    for (int i = 0; i < max; i++) {
      m.put((1 << 30) + i, i + inc);
      m.put((2 << 30) + i, i + inc);
      m.put((3 << 30) + i, i + inc);
    }

    org.junit.Assert.assertEquals(max * 3, m.size());

    int countSegments = 0;
    for (Object rawObj : m.partitionRootRec.values()) {
      Long[] segRecIds = (Long[]) rawObj;
      for (Long segRecId: segRecIds) {
        int[] segment = (int[]) ((Engine) m.engines.get(0)).get(
                segRecId,
                m.DIR_SERIALIZER);
        if (segment != null && segment.length > 4) {
          countSegments++;
        }
      }
    }

    org.junit.Assert.assertEquals(3, countSegments);

    keys = m.keySet(0).iterator();
    for (int i = 1; i <= 3; i++) {
      for (int j = 0; j < max; j++) {
        org.junit.Assert.assertTrue(keys.hasNext());
        keys.next();
        //TODO: to be fixed
        //System.out.println(((i << 30) + j) + "," + key);
        //assertEquals(Integer.valueOf((i << 30) + j), (Integer) key);
      }
    }
    org.junit.Assert.assertTrue(!keys.hasNext());
  }

  @Test
  public void testSingleIter() {

    Partitioner<String> partitioner = new Partitioner<String>(1) {
      @Override
      public int getPartition(String value) {
        return 0;
      }
    };

    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.STRING,
            Serializer.STRING,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    m.put("aa", "bb");
    Iterator iter = m.keySet(0).iterator();
    org.junit.Assert.assertTrue(iter.hasNext());
    org.junit.Assert.assertEquals("aa", iter.next());
    org.junit.Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void hasher() {

    Partitioner<int[]> partitioner = new Partitioner<int[]>(1) {
      @Override
      public int getPartition(int[] value) {
        return 0;
      }
    };

    PartitionedHTreeMap m = new PartitionedHTreeMap<int[], Object>(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.INT_ARRAY,
            Serializer.BASIC,
            null,
            null,
            false,
            Long.MAX_VALUE) {

      @Override
      public int hash(final int[] key) {
        return key[0];
      }
    };
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    //1e5
    for (int i = 0; i < 1e5; i++) {
      m.put(new int[]{i, i, i}, i);
    }

    for (Integer i = 0; i < 1e5; i++) {
      org.junit.Assert.assertEquals(i, m.get(new int[]{i, i, i}));
    }
  }

  @Test
  public void test_iterate_and_remove() {
    final int max = (int) 1e5;

    PartitionedHTreeMap m1 = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.INTEGER,
            null,
            null,
            null,
            false,
            Long.MAX_VALUE);
    m1.updateBucketLength(28);
    m1.updateDirectoryNodeSize(128, 32);

    Set m = m1.keySet(0);

    for (int i = 0; i < max; i++) {
      m.add(i);
    }


    Set control = new HashSet();
    Iterator iter = m.iterator();

    for (long i = 0; i < max / 2; i++) {
      org.junit.Assert.assertTrue(iter.hasNext());
      control.add(iter.next());
    }

    m.clear();

    while (iter.hasNext()) {
      control.add(iter.next());
    }
  }

  @Test
  public void testValueCreater() {

    Partitioner<String> partitioner = new Partitioner<String>(1) {
      @Override
      public int getPartition(String value) {
        return 0;
      }
    };

    final PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.STRING,
            null,
            new Fun.Function1<Integer, String>() {
              @Override
              public Integer run(String s) {
                return Integer.MIN_VALUE;
              }
            },
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    m.put("s2", 1);
    Integer v1 = (Integer) m.get("s1");
    org.junit.Assert.assertEquals(Integer.valueOf(Integer.MIN_VALUE), v1);
  }

  @Test
  public void slot_to_offset_long() {
    Random r = new Random();
    for (int i = 0; i < 1000; i++) {
      //fill array with random bites
      long[] l = new long[]{r.nextLong(), r.nextLong()};

      //turn bites into array pos
      List<Integer> b = new ArrayList();
      for (int j = 0; j < l.length; j++) {
        long v = l[j];
        for (int k = 0; k < 64; k++) {
          b.add((int) v & 1);
          v >>>= 1;
        }
      }
      org.junit.Assert.assertEquals(128, b.size());

      //iterate over an array, check if calculated pos equals

      int offset = 2;
      for (int slot = 0; slot < 128; slot++) {
        int current = b.get(slot);

        int coffset = PartitionedHTreeMap.dirOffsetFromSlot(l, slot);

        if (current == 0)
          coffset = -coffset;

        org.junit.Assert.assertEquals(offset, coffset);
        offset += current;
      }
    }
  }

  @Test
  public void slot_to_offset_int() {
    PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.STRING,
            null,
            new Fun.Function1<Integer, String>() {
              @Override
              public Integer run(String s) {
                return Integer.MIN_VALUE;
              }
            },
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    Random r = new Random();
    for (int i = 0; i < 1000; i++) {
      //fill array with random bites
      int[] l = new int[]{r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt()};

      //turn bites into array pos
      List<Integer> b = new ArrayList();
      for (int j = 0; j < l.length; j++) {
        long v = l[j];
        for (int k = 0; k < 32; k++) {
          b.add((int) v & 1);
          v >>>= 1;
        }
      }
      org.junit.Assert.assertEquals(128, b.size());

      //iterate over an array, check if calculated pos equals

      int offset = 4;
      for (int slot = 0; slot < 128; slot++) {
        int current = b.get(slot);

        int coffset = m.dirOffsetFromSlot(l, slot);

        if (current == 0)
          coffset = -coffset;

        org.junit.Assert.assertEquals(offset, coffset);
        offset += current;
      }
    }
  }

  @Test
  public void dir_put_long() {
    final PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.STRING,
            null,
            new Fun.Function1<Integer, String>() {
              @Override
              public Integer run(String s) {
                return Integer.MIN_VALUE;
              }
            },
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);
    for (int a = 0; a < 100; a++) {
      long[] reference = new long[127];
      Object dir = new int[4];
      Random r = new Random();
      for (int i = 0; i < 1e3; i++) {
        int slot = r.nextInt(127);
        long val = r.nextLong() & 0xFFFFFFF;

        if (i % 3 == 0 && reference[slot] != 0) {
          //delete every 10th element
          reference[slot] = 0;
          dir = m.dirRemove(dir, slot);
        } else {
          reference[slot] = val;
          dir = m.putNewRecordIdInDir(dir, slot, val);
        }

        //compare dir and reference
        long[] dir2 = new long[127];
        for (int j = 0; j < 127; j++) {
          int offset = m.dirOffsetFromSlot(dir, j);
          if (offset > 0)
            dir2[j] = PartitionedHTreeMap.dirGet(dir, offset);
        }

        org.junit.Assert.assertArrayEquals(reference, dir2);

        if (dir instanceof int[])
          org.junit.Assert.assertArrayEquals((int[]) dir, (int[]) UtilsTest.clone(dir,
                  m.DIR_SERIALIZER));
        else
          org.junit.Assert.assertArrayEquals((long[]) dir, (long[]) UtilsTest.clone(dir,
                  m.DIR_SERIALIZER));
      }
    }
  }

  @Test
  public void dir_put_int() {
    final PartitionedHTreeMap m = new PartitionedHTreeMap(
            0,
            "default",
            tempDirFile.toString(),
            "PartitionedHTreeMap",
            partitioner,
            false,
            2,
            Serializer.STRING,
            null,
            new Fun.Function1<Integer, String>() {
              @Override
              public Integer run(String s) {
                return Integer.MIN_VALUE;
              }
            },
            null,
            false,
            Long.MAX_VALUE);
    m.updateBucketLength(28);
    m.updateDirectoryNodeSize(128, 32);

    for (int a = 0; a < 100; a++) {
      long[] reference = new long[127];
      Object dir = new int[4];
      Random r = new Random();
      for (int i = 0; i < 1e3; i++) {
        int slot = r.nextInt(127);
        long val = r.nextInt((int) 1e6);

        if (i % 3 == 0 && reference[slot] != 0) {
          //delete every 10th element
          reference[slot] = 0;
          dir = m.dirRemove(dir, slot);
        } else {
          reference[slot] = val;
          dir = m.putNewRecordIdInDir(dir, slot, val);
        }

        //compare dir and reference
        long[] dir2 = new long[127];
        for (int j = 0; j < 127; j++) {
          int offset = m.dirOffsetFromSlot(dir, j);
          if (offset > 0)
            dir2[j] = PartitionedHTreeMap.dirGet(dir, offset);
        }

        org.junit.Assert.assertArrayEquals(reference, dir2);

        if (dir instanceof int[])
          org.junit.Assert.assertArrayEquals((int[]) dir, (int[]) UtilsTest.clone(dir,
                  m.DIR_SERIALIZER));
        else
          org.junit.Assert.assertArrayEquals((long[]) dir, (long[]) UtilsTest.clone(dir,
                  m.DIR_SERIALIZER));
      }
    }
  }
}
