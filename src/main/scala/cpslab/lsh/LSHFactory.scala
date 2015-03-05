package cpslab.lsh

import cpslab.deploy.LSHServer

object LSHFactory {

  /**
   * generate a new instance of LSH
   * @param name the name of the LSH schema
   * @return the LSH instance
   */
  def newInstance(name: String): Option[LSH] = name match {
    case x => None
  }
  
}
