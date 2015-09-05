package cpslab.deploy.benchmark

import scala.io.Source

import cpslab.deploy.{ShardDatabase, Utils}
import cpslab.lsh.vector.{SparseVector, Vectors}

trait DataSetLoader {

  /**
   * initialize the database by reading raw vector data from file system
   * @param filePath the root path of the data directory
   * @param replica this parameter controls the level we would like to scale the dataset
   * @param offset the offset for each replica
   * @param cap the maximum number of vectors we need
   */
  protected def initVectorDatabaseFromFS(
      filePath: String,
      replica: Int,
      offset: Int,
      cap: Int): Unit = {
    import ShardDatabase._
    def loadDataFromFS() {
      if (filePath != "") {
        val allFiles = Utils.buildFileListUnderDirectory(filePath)
        for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
          val (id, size, indices, values) = Vectors.fromString1(line)
          var currentOffset = 0
          for (i <- 0 until replica) {
            val vector = new SparseVector(id + currentOffset, size, indices, values)
            if (vectorIdToVector.size() > cap) {
              return
            }
            vectorIdToVector.put(vector.vectorId, vector)
            currentOffset += offset
          }
        }
      }
    }
    loadDataFromFS()
    println(s"Finished Loading Data, containing ${vectorIdToVector.size()} vectors")
  }
}
