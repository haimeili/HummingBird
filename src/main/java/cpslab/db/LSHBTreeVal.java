package cpslab.db;

import java.io.Serializable;

public class LSHBTreeVal implements Serializable {
  public int vectorId;
  public long hash;

  public LSHBTreeVal(int vId, long h) {
    vectorId = vId;
    hash = h;
  }
}
