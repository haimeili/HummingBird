package cpslab.deploy.benchmark

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.Lock

import cpslab.lsh.vector.SparseVector

class LockStrippingHashMap {
  val locks = new Array[Lock](196)

  val store = new mutable.HashMap[Int, ListBuffer[SparseVector]]

  def put(key: Int, value: SparseVector): Unit = {
    try {
      locks(key % 196).acquire()
      store.getOrElseUpdate(key, new ListBuffer[SparseVector]) += value
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 196).release()
    }
  }

  def get(key: Int) = {
    try {
      locks(key % 196).acquire()
      store.get(key)
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 196).release()
    }
  }
}
