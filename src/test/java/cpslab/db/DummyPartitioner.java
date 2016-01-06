package cpslab.db;

class DummyPartitioner extends Partitioner<Object> {

  public DummyPartitioner(int numPartitions) {
    super(numPartitions);
  }

  @Override
  public int getPartition(Object value) {
    return (int) ((Integer) value % numPartitions);
  }
}
