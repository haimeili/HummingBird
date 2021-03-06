package cpslab.db

import java.util.concurrent.Executors

import cpslab.TestSettings
import cpslab.deploy.{LSHServer, ShardDatabase}
import cpslab.lsh.LSH
import cpslab.lsh.vector.SparseVector
import cpslab.utils.{HashPartitioner, Serializers}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Ignore}

@Ignore
class PartitionedTreeSuite extends FunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
    // PartitionedHTreeMap.updateBucketLength(28)
    // PartitionedHTreeMap.updateDirectoryNodeSize(128)
    initLSHTables()
  }

  private def initLSHTables(): Unit = {
    ShardDatabase.vectorIdToVector  =
      new PartitionedHTreeMap(
        1,
        "default",
        s"${getClass.getClassLoader.getResource("testdir").getFile}-vector",
        "vectorIdToVector",
        new HashPartitioner[Int](2),
        true,
        1,
        Serializers.scalaIntSerializer,
        Serializers.vectorSerializer,
        null,
        Executors.newCachedThreadPool(),
        true,
        Int.MaxValue)
    ShardDatabase.vectorIdToVector.updateBucketLength(28)
    ShardDatabase.vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](1)
    ShardDatabase.vectorDatabase(0) = new PartitionedHTreeMap(
      0,
      "lsh",
      s"${getClass.getClassLoader.getResource("testdir").getFile}-vector",
      "lshtable",
      new HashPartitioner[Int](2),
      true,
      1,
      Serializers.scalaIntSerializer,
      null,
      null,
      Executors.newCachedThreadPool(),
      true,
      Int.MaxValue)
    ShardDatabase.vectorDatabase(0).updateDirectoryNodeSize(128, 32)
  }

  test("write the vector correctly") {
    ShardDatabase.vectorIdToVector.put(
      1, new SparseVector(1, 3, Seq(0, 2).toArray, Seq(1.0, 2.0).toArray))
    val v = ShardDatabase.vectorIdToVector.get(1)
    assert(v.vectorId === 1)
    assert(v.size === 3)
    assert(v.indices.toSeq === Seq(0, 2))
    assert(v.values.toSeq === Seq(1.0, 2.0))
  }

  private def testWriteAndGetSimilar(): Unit = {
    val vectorIdToVector = ShardDatabase.vectorIdToVector
    val vectorDB = ShardDatabase.vectorDatabase(0)
    vectorIdToVector.put(1, new SparseVector(1, 3, Seq(0, 2).toArray, Seq(1.0, 2.0).toArray))
    vectorIdToVector.put(2, new SparseVector(1, 3, Seq(0, 2).toArray, Seq(1.0, 2.0).toArray))
    vectorDB.put(1, true)
    vectorDB.put(2, true)
    val l = vectorDB.getSimilar(2)
    assert(l.size() == 1)
    val v = l.get(0)
    assert(v === 1)
  }

  test("write the vector and get similar correctly (128-length directory node)") {
    testWriteAndGetSimilar()
  }

  test("write the vector and get similar correctly (64-length directory node)") {
    initLSHTables()
    testWriteAndGetSimilar()
  }

  test("write the vector and get similar correctly (32-length directory node)") {
    initLSHTables()
    testWriteAndGetSimilar()
  }
}
