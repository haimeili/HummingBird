package cpslab.deploy.benchmark

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import cpslab.lsh.vector.SparseVector

class LockStrippingHashMap {
  val locks = Array.fill[ReentrantLock](8192)(new ReentrantLock)

  val store = new mutable.HashMap[Int, ListBuffer[SparseVector]]

  def getAndPut(key: Int, value: SparseVector): Unit = {
    try {
      locks(key % 8192).lock()
      store.get(key)
      store.getOrElseUpdate(key, new ListBuffer[SparseVector]) += value
    } catch {
      case x: Exception =>
        x.printStackTrace()
    } finally {
      locks(key % 8192).unlock()
    }
  }
}
