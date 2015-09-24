package cpslab.deploy.benchmark

import scala.io.Source

import cpslab.deploy.ShardDatabase._
import cpslab.deploy.{ShardDatabase, Utils}
import cpslab.lsh.vector.{SparseVector, Vectors}

trait DataSetLoader {

  private[benchmark] def loadDataFromFS(filePath: String, cap: Int, tableNum: Int) {
    if (filePath != "") {
      val allFiles = Utils.buildFileListUnderDirectory(filePath)
      var cnt = 0
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        val (id, size, indices, values) = Vectors.fromString1(line)
        val vector = new SparseVector(cnt, size, indices, values)
        if (vectorIdToVector.size() > cap) {
          return
        }
        vectorIdToVector.put(vector.vectorId, vector)
        for (i <- 0 until tableNum) {
          vectorDatabase(i).put(vector.vectorId, true)
        }
        cnt += 1
      }
    }
  }

  /**
   * initialize the database by reading raw vector data from file system
   * @param filePath the root path of the data directory
   * @param cap the maximum number of vectors we need
   */
  def initVectorDatabaseFromFS(
      filePath: String,
      cap: Int,
      tableNum: Int): Unit = {
    import ShardDatabase._

    this.loadDataFromFS(filePath, cap, tableNum)
    println(s"Finished Loading Data, containing ${vectorIdToVector.size()} vectors")
  }
}
