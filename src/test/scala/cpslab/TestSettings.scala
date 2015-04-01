package cpslab

import com.typesafe.config.ConfigFactory

private[cpslab] object TestSettings {
  private val appConf = ConfigFactory.parseString(
    s"""
       |cpslab.lsh.similarityThreshold = 0.0
       |cpslab.lsh.vectorDim = 3
       |cpslab.lsh.chainLength = 10
       |cpslab.lsh.familySize = 100
       |cpslab.lsh.topK = 2
       |cpslab.lsh.plsh.maxWorkerNum = 10
       |cpslab.lsh.plsh.partitionSwitch=false
       |cpslab.lsh.tableNum = 10
       |cpslab.lsh.deploy.client = "/user/client"
       |cpslab.lsh.generateMethod = default
       |cpslab.lsh.nodeID = 0
       |cpslab.lsh.name = pStable
       |cpslab.lsh.familySize = 10
       |cpslab.lsh.family.pstable.mu = 0.0
       |cpslab.lsh.family.pstable.sigma = 0.02
       |cpslab.lsh.family.pstable.w = 3
       """.stripMargin)

  private val akkaConf = ConfigFactory.parseString(
    """
      |akka.loglevel = "INFO"
      |akka.remote.netty.tcp.port = 0
      |akka.remote.netty.tcp.hostname = "127.0.0.1"
      |akka.cluster.roles = [compute]
      |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
      |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      |akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    """.stripMargin)

  private val shardingConf = ConfigFactory.parseString(
    """
      |cpslab.lsh.distributedSchema = SHARDING
      |cpslab.lsh.sharding.namespace = flat
      |cpslab.lsh.sharding.maxShardNumPerTable = 100
      |cpslab.lsh.sharding.maxShardDatabaseWorkerNum = 1
      |cpslab.lsh.sharding.loadBatchingDuration = 0
      |cpslab.lsh.sharding.maxDatabaseNodeNum = 1
      |cpslab.lsh.writerActorNum = 10
      |cpslab.lsh.sharding.loadBatchingDuration = 0
      |cpslab.lsh.nodeID = 0
    """.stripMargin)

  val testBaseConf = appConf.withFallback(akkaConf)
  val testShardingConf = appConf.withFallback(akkaConf).withFallback(shardingConf)
}
