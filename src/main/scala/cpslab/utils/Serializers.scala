package cpslab.utils

import java.io.{DataInput, DataOutput}

import cpslab.db.{LSHBTreeVal, Serializer}
import cpslab.lsh.vector.SparseVector

object Serializers {
  val scalaIntSerializer = new Serializer[Int] {

    override def serialize(out: DataOutput, value: Int): Unit = {
      out.writeInt(value)
    }

    override def deserialize(in: DataInput, available: Int): Int = {
      in.readInt()
    }
  }

  val vectorIDHashPairSerializer = new Serializer[LSHBTreeVal] {
    override def serialize(out: DataOutput, obj: LSHBTreeVal): Unit = {
      val vectorId = obj.vectorId
      val hash = obj.hash
      out.writeInt(vectorId)
      out.writeInt(hash)
    }

    override def deserialize(in: DataInput, available: Int): LSHBTreeVal = {
      val vectorId = in.readInt()
      val hash = in.readInt()
      new LSHBTreeVal(vectorId, hash)
    }
  }

  val vectorSerializer = new Serializer[SparseVector] {

    override def serialize(out: DataOutput, obj: SparseVector): Unit = {
      out.writeInt(obj.vectorId)
      out.writeInt(obj.size)
      out.writeInt(obj.indices.length)
      for (idx <- obj.indices) {
        out.writeInt(idx)
      }
      for (value <- obj.values) {
        out.writeDouble(value)
      }
    }

    override def deserialize(in: DataInput, available: Int): SparseVector = {
      val vectorId = in.readInt()
      val size = in.readInt()
      val realSize = in.readInt()
      val indices = for (i <- 0 until realSize) yield in.readInt()
      val values = for (i <- 0 until realSize) yield in.readDouble()
      new SparseVector(vectorId, size, indices.toArray, values.toArray)
    }
  }

  def IntSerializer = scalaIntSerializer
  def VectorSerializer = vectorSerializer
  def VectorIDHashSerializer = vectorIDHashPairSerializer
}