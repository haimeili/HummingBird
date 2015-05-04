package cpslab.deploy

import scala.collection.mutable.ListBuffer

import com.typesafe.config.Config
import cpslab.lsh.vector.SparseVector
import org.mapdb.DBMaker.Maker
import org.mapdb.{DBMaker, HTreeMap, Serializer}

private[deploy] object ShardDatabase {

  private def initDBMaker(conf: Config): Maker = {
    var dbMaker = DBMaker.
      memoryDirectDB().
      transactionDisable()
    val asyncDelay = conf.getInt("cpslab.vectorDatabase.asyncDelay")
    if (asyncDelay > 0) {
      val asyncQueueSize = conf.getInt("cpslab.vectorDatabase.asyncQueueSize")
      dbMaker = dbMaker.asyncWriteEnable().asyncWriteFlushDelay(asyncDelay).
        asyncWriteQueueSize(asyncQueueSize)
    }
    dbMaker
  }

  def initializeMapDBHashMap(conf: Config): Unit = {
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    def initializeVectorDatabase(): HTreeMap[Int, ListBuffer[Int]] =  {
      val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
        counterEnable().
        keySerializer(Serializer.INTEGER)
      hashMapMaker.make[Int, ListBuffer[Int]]()
    }
    def initializeIdToVectorMap(): HTreeMap[Int, SparseVector] = {
      val hashMapMaker = DBMaker.hashMapSegmented(initDBMaker(conf)).
        counterEnable().
        keySerializer(Serializer.INTEGER)
      hashMapMaker.make[Int, SparseVector]()
    }
    vectorDatabase = Array.fill(tableNum)(initializeVectorDatabase())
    vectorIdToVector = initializeIdToVectorMap()
  }

  private[deploy] var vectorDatabase: Array[HTreeMap[Int, ListBuffer[Int]]] = null
  private[deploy] var vectorIdToVector: HTreeMap[Int, SparseVector] = null

}
