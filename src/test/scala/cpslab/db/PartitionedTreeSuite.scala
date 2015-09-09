package cpslab.db

import java.util.concurrent.Executors

import cpslab.lsh.vector.SparseVector
import cpslab.utils.{Serializers, HashPartitioner}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class PartitionedTreeSuite extends FunSuite with BeforeAndAfterAll {

  var vectorIdToVector: PartitionedHTreeMap[Int, SparseVector] = null
  //var lshTable: PartitionedHTreeMap = null

  override def beforeAll(): Unit = {
    vectorIdToVector =
      new PartitionedHTreeMap(
        0,
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
  }

  test("write the vector correctly") {
    vectorIdToVector.put(1, new SparseVector(1, 3, Seq(0, 2).toArray, Seq(1.0, 2.0).toArray))
    val v = vectorIdToVector.get(1)
  }

}
