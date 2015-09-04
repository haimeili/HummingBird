package cpslab.db;

public abstract class Partitioner<A> {

  public abstract int getPartition(A value);

  public int numPartitions;

  public Partitioner(int numPartitions) {
    this.numPartitions = numPartitions;
  }

}
