package cpslab.db;

import cpslab.lsh.vector.SparseVector;
import cpslab.utils.HashPartitioner;
import cpslab.utils.Serializers;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;

@SuppressWarnings({"unchecked", "rawtypes"})
public class DirectoryNodeSuite {
  static DummyPartitioner partitioner = new DummyPartitioner(1);

  static Path tempDirFile;

  @BeforeClass
  public static void onlyOnce() {
    try {
      tempDirFile = Files.createTempDirectory(String.valueOf(System.currentTimeMillis()));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test_simple_put_with_128() {
    PartitionedHTreeMap.updateBucketLength(28);
    PartitionedHTreeMap.updateDirectoryNodeSize(128);
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
  public void test_simple_put_with_64() {
    PartitionedHTreeMap.updateBucketLength(30);
    PartitionedHTreeMap.updateDirectoryNodeSize(64);
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
  public void test_simple_put_with_32() {
    PartitionedHTreeMap.updateBucketLength(30);
    PartitionedHTreeMap.updateDirectoryNodeSize(32);
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

}
