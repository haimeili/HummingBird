package cpslab.deploy.benchmark

import scala.io.Source

import cpslab.deploy.{ShardDatabase, Utils}
import cpslab.lsh.vector.{SparseVector, Vectors}

trait DataSetLoader {

  /**
   * initialize the database by reading raw vector data from file system
   * @param filePath the root path of the data directory
   */
  protected def initVectorDatabaseFromFS(
      filePath: String,
      replica: Int,
      offset: Int): Unit = {
    import ShardDatabase._
    if (filePath != "") {
      val allFiles = Utils.buildFileListUnderDirectory(filePath)
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        val (id, size, indices, values) = Vectors.fromString1(line)
        var currentOffset = 0
        for (i <- 0 until replica) {
          val vector = new SparseVector(id + currentOffset, size, indices, values)
          vectorIdToVector.put(vector.vectorId, vector)
          currentOffset += offset
        }
      }
    }
    println(s"Finished Loading Data, containing ${vectorIdToVector.size()} vectors")
  }
}
