package cpslab.deploy

import org.scalatest.FunSuite

class UtilsSuite extends FunSuite {

  test("generate sub file path correctly") {
    val testDir = getClass.getClassLoader.getResource("testdir").getFile
    val allFiles = Utils.buildFileListUnderDirectory(testDir)
    assert(allFiles.length === 3)
    assert(allFiles(0).charAt(allFiles(0).length - 1) === 'a')
    assert(allFiles(1).charAt(allFiles(1).length - 1) === 'b')
    assert(allFiles(2).charAt(allFiles(2).length - 1) === 'c')
  }
}
