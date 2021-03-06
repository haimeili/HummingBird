/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cpslab.lsh.vector

import java.lang.{Double => JavaDouble, Integer => JavaInteger, Iterable => JavaIterable}
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}

// added this file to eliminate the dependency to spark (causing sbt
// assembly extremely slow)

sealed trait Vector extends Serializable {

  /**
   * Size of the vector.
   */
  def size: Int

  /**
   * Converts the instance to a double array.
   */
  def toArray: Array[Double]

  override def equals(other: Any): Boolean = {
    other match {
      case v: Vector =>
        util.Arrays.equals(this.toArray, v.toArray)
      case _ => false
    }
  }

  override def hashCode(): Int = util.Arrays.hashCode(this.toArray)

  /**
   * Makes a deep copy of this vector.
   */
  def copy: Vector = {
    throw new NotImplementedError(s"copy is not implemented for ${this.getClass}.")
  }
}

object Vectors {

  private val vectorId = new AtomicInteger(0)

  def nextVectorID: Int = vectorId.getAndIncrement

  /**
   * Creates a dense vector from its values.
   */
  @varargs
  def dense(firstValue: Double, otherValues: Double*): Vector =
    new DenseVector(nextVectorID, (firstValue +: otherValues).toArray)

  // A dummy implicit is used to avoid signature collision with the one generated by @varargs.
  /**
   * Creates a dense vector from a double array.
   */
  def dense(values: Array[Double]): Vector = new DenseVector(nextVectorID, values)

  /**
   * Creates a dense vector from a double array and its ID
   * @param id the vector id
   * @param values the values in this dense vector
   * @return the newly created vector
   */
  def dense(id: Int, values: Array[Double]): Vector = new DenseVector(id, values)

  /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): Vector =
    new SparseVector(Vectors.nextVectorID, size, indices, values)

  /**
   * Creates a sparse vector providing its id, index array and value array.
   *
   * @param id vector Id
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  def sparse(id: Int, size: Int, indices: Array[Int], values: Array[Double]): Vector =
    new SparseVector(id, size, indices, values)

  /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector = {
    require(size > 0)

    val (indices, values) = elements.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size)

    new SparseVector(Vectors.nextVectorID, size, indices.toArray, values.toArray)
  }

  /**
   * Creates a sparse vector using unordered (index, value) pairs in a Java friendly way.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: JavaIterable[(JavaInteger, JavaDouble)]): Vector = {
    sparse(size, elements.asScala.map { case (i, x) =>
      (i.intValue(), x.doubleValue())
    }.toSeq)
  }

  /**
   * Creates a dense vector of all zeros.
   *
   * @param size vector size
   * @return a zero vector
   */
  def zeros(size: Int): Vector = {
    new DenseVector(Vectors.nextVectorID, new Array[Double](size))
  }

  private[cpslab] def fromString1(inputString: String): (Int, Int, Array[Int], Array[Double]) = {
    val stringArray = inputString.split(",\\[")
    if (stringArray.length != 3) {
      throw new Exception(s"cannot parse $inputString")
    }
    val size = stringArray(0).replace("(", "").toInt
    val indices = stringArray(1).replace("]", "").split(",").map(_.toInt)
    val Array(valuesStr, idStr) = stringArray(2).split("\\]\\),")
    val values = valuesStr.split(",").map(_.toDouble)
    val id = idStr.replace(")", "").toInt
    (id, size, indices, values)
  }

  private[cpslab] def fromString(inputString: String): (Int, Int, Array[Int], Array[Double]) = {
    val stringArray = inputString.split(",\\[")
    if (stringArray.length != 3) {
      throw new Exception(s"cannot parse $inputString")
    }
    val Array(id, size) = stringArray(0).replace("(", "").split(",").map(_.toInt)
    val indicesStringArray = stringArray(1).replace("]", "").split(",").filter(_ != "")
    val indices = if (indicesStringArray.nonEmpty) indicesStringArray.map(_.toInt) else
      new Array[Int](0)
    val valueStringArray = stringArray(2).replace("])", "").split(",").filter(_ != "")
    val values = if (valueStringArray.nonEmpty) valueStringArray.map(_.toDouble) else
      new Array[Double](0)
    (id, size, indices, values)
  }

  private[cpslab] def parseNumeric(any: Any): Vector = {
    any match {
      case values: Array[Double] =>
        Vectors.dense(values)
      case Seq(size: Double, indices: Array[Double], values: Array[Double]) =>
        Vectors.sparse(size.toInt, indices.map(_.toInt), values)
      case vectorString: String =>
        //only support sparseVectors for now
        val parsedResult = fromString(vectorString)
        Vectors.sparse(parsedResult._1, parsedResult._2, parsedResult._3, parsedResult._4)
      case other =>
        throw new Exception(s"Cannot parse $other.")
    }
  }

  /**
   * Creates a vector instance from a breeze vector.
   */
  private[cpslab] def fromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1) {
          new DenseVector(Vectors.nextVectorID, v.data)
        } else {
          // Can't use underlying array directly, so make a new one
          new DenseVector(Vectors.nextVectorID, v.toArray)
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SparseVector(Vectors.nextVectorID, v.length, v.index, v.data)
        } else {
          new SparseVector(Vectors.nextVectorID, v.length, v.index.slice(0, v.used),
            v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }
}

class DenseVector(val vectorId: Int, val values: Array[Double]) extends Vector {

  override def size: Int = values.length

  override def toString: String = values.mkString("[", ",", "]")

  override def toArray: Array[Double] = values

  override def copy: DenseVector = {
    new DenseVector(vectorId, values.clone())
  }
}

class SparseVector(
    val vectorId: Int,
    override val size: Int,
    val indices: Array[Int],
    val values: Array[Double]) extends Vector {

  def this(paraTuple: (Int, Int, Array[Int], Array[Double])) =
    this(paraTuple._1, paraTuple._2, paraTuple._3, paraTuple._4)

  require(indices.length == values.length,
    s"indices length: ${indices.length}, values length: ${values.length}")
  
  val indexToMap = new mutable.HashMap[Int, Double]()

  for (i <- 0 until indices.length) {
    indexToMap(indices(i)) = values(i)
  }
  
  val bitVector: util.BitSet = {
    val bv = new util.BitSet()
    for (i <- indices) {
      bv.set(i)
    }
    bv
  }
  
  override def toString: String =
    "(%s,%s,%s,%s)".format(vectorId, size, indices.mkString("[", ",", "]"),
      values.mkString("[", ",", "]"))

  override def toArray: Array[Double] = {
    val data = new Array[Double](size)
    var i = 0
    val nnz = indices.length
    while (i < nnz) {
      data(indices(i)) = values(i)
      i += 1
    }
    data
  }

  override def copy: SparseVector = {
    new SparseVector(vectorId, size, indices.clone(), values.clone())
  }
}
