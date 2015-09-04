package cpslab.utils

import java.io.{DataInput, DataOutput}

import cpslab.db.Serializer
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

  val vectorSerializer = new Serializer[SparseVector] {

    override def serialize(out: DataOutput, value: SparseVector): Unit = {
      val vectorId = out.writeInt(value.vectorId)
      val size = out.writeInt(value.size)
      for (idx <- value.indices) {
        out.writeInt(idx)
      }
      for (value <- value.values) {
        out.writeDouble(value)
      }
    }

    override def deserialize(in: DataInput, available: Int): SparseVector = {
      val vectorId = in.readInt()
      val size = in.readInt()
      val indices = for (i <- 0 until size) yield in.readInt()
      val values = for (i <- 0 until size) yield in.readDouble()
      new SparseVector(vectorId, size, indices.toArray, values.toArray)
    }
  }

  def IntSerializer = scalaIntSerializer
  def VectorSerializer = vectorSerializer
}