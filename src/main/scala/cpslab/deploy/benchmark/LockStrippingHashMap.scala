package cpslab.deploy.benchmark

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.lsh.vector.SparseVector

class LockStrippingHashMap {
  val locks = Array.fill[ReentrantReadWriteLock](8192)(new ReentrantReadWriteLock(false))

  val store = new mutable.HashMap[Int, ListBuffer[SparseVector]]

  def put(key: Int, value: SparseVector): Unit = {
    try {
      locks(key % 8192).writeLock().lock()
      store.getOrElseUpdate(key, new ListBuffer[SparseVector]) += value
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 8192).writeLock().unlock()
    }
  }

  def get(key: Int) = {
    try {
      locks(key % 8192).readLock().lock()
      store.get(key)
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 8192).readLock().unlock()
    }
  }
}
