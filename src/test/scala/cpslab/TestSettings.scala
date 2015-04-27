package cpslab

import com.typesafe.config.ConfigFactory

private[cpslab] object TestSettings {
  private val appConf = ConfigFactory.parseString(
    s"""
       |cpslab.lsh.plsh.benchmark.expDuration=0
       |cpslab.lsh.similarityThreshold = 0.0
       |cpslab.lsh.vectorDim = 3
       |cpslab.lsh.chainLength = 10
       |cpslab.lsh.familySize = 100
       |cpslab.lsh.topK = 2
       |cpslab.lsh.plsh.updateWindowSize = 10
       |cpslab.lsh.plsh.partitionSwitch=false
       |cpslab.lsh.plsh.maxNumberOfVector=100
       |cpslab.lsh.initVectorNumber=0
       |cpslab.lsh.plsh.maxWorkerNum=5
       |cpslab.lsh.inputFilePath=""
       |cpslab.lsh.tableNum = 10
       |cpslab.lsh.deploy.client = "/user/client"
       |cpslab.lsh.generateMethod = default
       |cpslab.lsh.nodeID = 0
       |cpslab.lsh.name = pStable
       |cpslab.lsh.deploy.maxNodeNum=100
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
      |cpslab.lsh.sharding.maxShardNumPerTable = 100
      |cpslab.lsh.sharding.maxShardDatabaseWorkerNum = 1
      |cpslab.lsh.sharding.loadBatchingDuration = 0
      |cpslab.lsh.sharding.maxDatabaseNodeNum = 1
      |cpslab.lsh.writerActorNum = 10
      |cpslab.lsh.sharding.loadBatchingDuration = 0
      |cpslab.lsh.nodeID = 0
    """.stripMargin)

  private val clientConf = ConfigFactory.parseString(
    """
      |cpslab.lsh.plsh.benchmark.inputSource=""
      |cpslab.lsh.plsh.benchmark.remoteProxyList=["akka.tcp://LSH@127.0.0.1:3000/user/clientRequestHandler"]
      |cpslab.lsh.plsh.workerList=["akka.tcp://LSH@127.0.0.1:3000/user/PLSHWorker", "akka.tcp://LSH@127.0.0.1:3001/user/PLSHWorker"]
      |cpslab.lsh.plsh.benchmark.messageInterval=200
    """.stripMargin)

  val testBaseConf = appConf.withFallback(akkaConf)
  val testShardingConf = appConf.withFallback(akkaConf).withFallback(shardingConf)
  val testClientConf = appConf.withFallback(akkaConf).withFallback(clientConf)
}
