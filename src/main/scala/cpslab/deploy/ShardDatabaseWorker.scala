package cpslab.deploy

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{Actor, Cancellable, Props}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import com.typesafe.config.Config
import cpslab.deploy.ShardDatabase._
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, SparseVectorWrapper}

private[deploy] class ShardDatabaseWorker(conf: Config, lshInstance: LSH) extends Actor {

  //sharding actors
  private val regionActor = ClusterSharding(context.system).shardRegion(
    ShardDatabaseWorker.shardDatabaseWorkerActorName)
  //sharding configuration
  private val maxShardNumPerTable = conf.getInt("cpslab.lsh.sharding.maxShardNumPerTable")

  // data structures for message batching
  private var batchingSendingTask: Cancellable = null
  private val loadBatchingDuration = conf.getLong("cpslab.lsh.sharding.loadBatchingDuration")
  private lazy val flatAllocationWriteBuffer = new ConcurrentHashMap[ShardId,
    mutable.HashMap[SparseVectorWrapper, Array[Int]]]

  //similarity calculation setup
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")

  //client end
  //TODO: change for dynamic client actor
  private val clientActorAddress = conf.getString("cpslab.lsh.clientAddress")
  private val clientActor = context.actorSelection(clientActorAddress)

  override def preStart(): Unit = {
    // initialize the sender for load batching
    if (loadBatchingDuration > 0) {
      val system = context.system
      import system.dispatcher
      batchingSendingTask = context.system.scheduler.schedule(
        0 milliseconds, loadBatchingDuration milliseconds, new Runnable {
          override def run(): Unit = {
            sendShardAllocation()
          }
        })
    }
  }

  override def postStop(): Unit = {
    if (batchingSendingTask != null) {
      batchingSendingTask.cancel()
    }
  }

  /**
   * send ShardAllocation to regionActors
   */
  private def sendShardAllocation(): Unit = {
    for ((shardId, perShardMap) <- flatAllocationWriteBuffer) {
      regionActor ! FlatShardAllocation(
        HashMap[ShardId, mutable.HashMap[SparseVectorWrapper, Array[Int]]]((shardId, perShardMap)))
    }
    flatAllocationWriteBuffer.clear()
  }

  /**
   * called after the ShardAllocation has been calculated for a certain message
   * merge the shard allocation for a message to the write buffer or send it immediately
   * @param outputShardMap the shard allocation map for a message to be merged to the write buffer
   */
  private def sendOrBatchShardAllocation(
      outputShardMap: mutable.HashMap[ShardId, mutable.HashMap[SparseVectorWrapper, Array[Int]]]):
      Unit = {
    for ((shardId, tableMap) <- outputShardMap) {
      if (loadBatchingDuration <= 0) {
        regionActor ! FlatShardAllocation(
          HashMap[ShardId, mutable.HashMap[SparseVectorWrapper, Array[Int]]]((shardId, tableMap)))
      } else {
        //merge to write buffer
        for ((vector, tableId) <- tableMap) {
          val vectorsInBatching = flatAllocationWriteBuffer.
            getOrElseUpdate(shardId, new mutable.HashMap[SparseVectorWrapper, Array[Int]]).
            getOrElseUpdate(vector, tableId)
        }
      }
    }
  }

  /**
   * processing logic for search request
   * @param searchRequest the search requeste received from client
   */
  private def processSearchRequest(searchRequest: SearchRequest): Unit = {
    val indexInAllTables = lshInstance.calculateIndex(searchRequest.vector)
    val outputShardMap = new mutable.HashMap[ShardId,
      mutable.HashMap[SparseVectorWrapper, Array[Int]]]
    val tableNum = indexInAllTables.length
    val indexInTable = new Array[Int](indexInAllTables.length)
    val vector = SparseVectorWrapper(indexInTable, searchRequest.vector)
    for (tableId <- 0 until tableNum) {
      indexInTable(tableId) = indexInAllTables(tableId)
      val bucketIndex = indexInAllTables(tableId)
      val shardId = bucketIndex % maxShardNumPerTable
      outputShardMap.getOrElseUpdate(shardId.toString,
        new mutable.HashMap[SparseVectorWrapper, Array[Int]]).
        getOrElseUpdate(vector, Array.fill[Int](tableNum)(-1))(tableId) = tableId
    }
    sendOrBatchShardAllocation(outputShardMap)
  }

  private def generateSimilarityOutputs(shardAllocation: FlatShardAllocation):
      Iterable[SimilarityOutput] =  {
    val shardMap = shardAllocation.shardsMap
    val deduplicateBitmap = new mutable.HashMap[SparseVector, util.BitSet]
    for ((_, withinShardData) <- shardMap;
         (vector, tableIds) <- withinShardData;
         tableId <- tableIds if tableId != -1) {
      //query and update the database
      val allSimilarCandidates = vectorDatabase(tableId).get(vector.bucketIndices(tableId))
      val bitMap = deduplicateBitmap.getOrElseUpdate(vector.sparseVector, new util.BitSet)
      if (allSimilarCandidates != null) {
        allSimilarCandidates.foreach(candidateVectorID => bitMap.set(candidateVectorID))
      }
    }
    deduplicateBitmap.map {case (queryVector, bitmap) =>
      var nextSetBit = bitmap.nextSetBit(0)
      val selectedCandidates = new ListBuffer[(Int, Double)]
      while (nextSetBit != -1) {
        val candidatesVector = vectorIdToVector.get(nextSetBit)
        val similarity = SimilarityCalculator.fastCalculateSimilarity(queryVector, candidatesVector)
        if (similarity >= similarityThreshold) {
          selectedCandidates += nextSetBit -> similarity
        }
        nextSetBit = bitmap.nextSetBit(nextSetBit + 1)
      }
      if (selectedCandidates.size > 0) {
        val topKCandidate = selectedCandidates.sortWith((a, b) => a._2 < b._2).take(topK)
        SimilarityOutput(queryVector.vectorId, null, topKCandidate.toList)
      } else {
        null
      }
    }.filter(_ != null)
  }

  /**
   * process ShardAllocation message
   * query the similar candidates and update the similarity index
   * @param shardAllocationMsg the shardAllocation Message
   */
  private def processShardAllocation(shardAllocationMsg: FlatShardAllocation): Unit = {
    val similarityOutputMessages = generateSimilarityOutputs(shardAllocationMsg)
    for (similarityOutput <- similarityOutputMessages) {
      clientActor ! similarityOutput
    }
    //update vector database
    updateVectorDatabase(shardAllocationMsg)
  }

  private def updateVectorDatabase(shardAllocationMsg: FlatShardAllocation): Unit = {
    val withInShardData = shardAllocationMsg.shardsMap.values
    for (vectorAndTableIDs <- withInShardData;
        (vector, tableIds) <- vectorAndTableIDs) {
      vectorIdToVector.put(vector.sparseVector.vectorId, vector.sparseVector)
      for (tableId <- tableIds if tableId != -1) {
        if (!vectorDatabase(tableId).containsKey(vector.bucketIndices(tableId))) {
          vectorDatabase(tableId).put(vector.bucketIndices(tableId), new ListBuffer[Int])
        }
        val list = vectorDatabase(tableId)(vector.bucketIndices(tableId))
        list += vector.sparseVector.vectorId
        vectorDatabase(tableId).put(vector.bucketIndices(tableId), list)
      }
    }
  }

  override def receive: Receive = {
    case searchRequest @ SearchRequest(_) =>
      processSearchRequest(searchRequest)
    case shardAllocation: FlatShardAllocation =>
      processShardAllocation(shardAllocation)
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorkerActor"
  def props(conf: Config, lsh: LSH) = Props(new ShardDatabaseWorker(conf, lsh))
}
