package cpslab.db;

import java.io.Serializable;

public class LSHBTreeVal implements Serializable {
  public int vectorId;
  public int hash;

  public LSHBTreeVal(int vId, int h) {
    vectorId = vId;
    hash = h;
  }
}
