package cpslab.deploy.benchmark

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.{ReentrantLock, Condition, Lock}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.lsh.vector.SparseVector

class LockStrippingHashMap {
  val locks = Array.fill[Lock](8192)(new ReentrantLock)

  val store = new mutable.HashMap[Int, ListBuffer[SparseVector]]

  def put(key: Int, value: SparseVector): Unit = {
    try {
      locks(key % 8192).lock()
      store.getOrElseUpdate(key, new ListBuffer[SparseVector]) += value
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 8192).unlock()
    }
  }

  def get(key: Int) = {
    try {
      locks(key % 8192).lock()
      store.get(key)
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 8192).unlock()
    }
  }
}
