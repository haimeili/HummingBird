package cpslab.deploy

import java.io.File

private[deploy] object Utils {

  def buildFileListUnderDirectory(directoryPath: String): Seq[String] = {
    val dirObj = new File(directoryPath)
    val regularFiles = dirObj.listFiles()
    regularFiles.view.filter(_.isFile).map(_.getAbsolutePath) ++
      regularFiles.view.filter(_.isDirectory).map(_.getAbsolutePath).flatMap(
        buildFileListUnderDirectory)
  }
}
