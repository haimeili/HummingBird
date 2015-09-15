package cpslab.deploy

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.{ReceiveTimeout, Actor, Cancellable, Props}
import akka.contrib.pattern.ClusterSharding
import akka.contrib.pattern.ShardRegion._
import com.typesafe.config.Config
import cpslab.db.PartitionedHTreeMap
import cpslab.deploy.ShardDatabase._
import cpslab.lsh.LSH
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, SparseVectorWrapper}

private[deploy] class ShardDatabaseWorker(conf: Config, lshInstance: LSH) extends Actor {

  import ShardDatabaseWorker._

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

  private val expDuration = conf.getLong("cpslab.lsh.benchmark.expDuration")
  if (expDuration > 0) {
    context.setReceiveTimeout(expDuration milliseconds)
  }

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
    import scala.collection.JavaConversions._
    flatAllocationWriteBuffer.synchronized {
      for ((shardId, perShardMap) <- flatAllocationWriteBuffer) {
        regionActor ! FlatShardAllocation(
          HashMap[ShardId, mutable.HashMap[SparseVectorWrapper, Array[Int]]]((shardId, perShardMap)))
      }
      flatAllocationWriteBuffer.clear()
    }
  }

  /**
   * called after the ShardAllocation has been calculated for a certain message
   * merge the shard allocation for a message to the write buffer or send it immediately
   * @param outputShardMap the shard allocation map for a message to be merged to the write buffer
   */
  private def sendOrBatchShardAllocation(
      outputShardMap: mutable.HashMap[ShardId, mutable.HashMap[SparseVectorWrapper, Array[Int]]]):
      Unit = {
    import scala.collection.JavaConversions._
    for ((shardId, tableMap) <- outputShardMap) {
      if (loadBatchingDuration <= 0) {
        regionActor ! FlatShardAllocation(
          HashMap[ShardId, mutable.HashMap[SparseVectorWrapper, Array[Int]]]((shardId, tableMap)))
      } else {
        //merge to write buffer
        for ((vector, tableId) <- tableMap) {
          flatAllocationWriteBuffer.
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
    //record start time
    val startMoment = System.nanoTime()
    val queryId = searchRequest.vector.vectorId
    startTime += queryId -> startMoment
    val indexInAllTables = lshInstance.calculateIndex(searchRequest.vector)
    val outputShardMap = new mutable.HashMap[ShardId,
      mutable.HashMap[SparseVectorWrapper, Array[Int]]]
    val tableNum = indexInAllTables.length
    val indexInTable = new Array[Int](indexInAllTables.length).view.zipWithIndex.map{
      case (num, idx) => indexInAllTables(idx)}
    val vector = SparseVectorWrapper(indexInTable.toArray, searchRequest.vector)
    for (tableId <- 0 until tableNum) {
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
      val db = vectorDatabase(tableId).asInstanceOf[PartitionedHTreeMap[Int, Boolean]]
      val allSimilarCandidates = db.getSimilar(vector.sparseVector)
      val bitMap = deduplicateBitmap.getOrElseUpdate(vector.sparseVector, new util.BitSet)
      if (allSimilarCandidates != null) {
        val itr = allSimilarCandidates.iterator()
        while (itr.hasNext) {
          bitMap.set(itr.next())
        }
      }
    }
    deduplicateBitmap.view.map {case (queryVector, bitmap) =>
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
        //val topKCandidate = selectedCandidates.sortWith((a, b) => a._2 < b._2).take(topK)
        val topKCandidate = selectedCandidates.take(topK)
        SimilarityOutput(queryVector.vectorId, null, topKCandidate.toList,
          Some(System.currentTimeMillis()))
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
    val shardMap = shardAllocationMsg.shardsMap
    val time = System.nanoTime()
    for ((_, withinShardData) <- shardMap; (vector, tableIds) <- withinShardData) {
      startTime += vector.sparseVector.vectorId -> time
    }

    val similarityOutputMessages = generateSimilarityOutputs(shardAllocationMsg)
    val currentTime = System.nanoTime()
    for (similarityOutput <- similarityOutputMessages) {
      val queryVectorID = similarityOutput.queryVectorID
      searchCost += queryVectorID -> (currentTime - startTime(queryVectorID))
      clientActor ! similarityOutput
    }
    //update vector database
    updateVectorDatabase(shardAllocationMsg)
  }

  private def updateVectorDatabase(shardAllocationMsg: FlatShardAllocation): Unit = {
    val withInShardData = shardAllocationMsg.shardsMap.values
    val writingTimeMoment = System.nanoTime()
    for (vectorAndTableIDs <- withInShardData;
        (vector, tableIds) <- vectorAndTableIDs) {
      val vectorId = vector.sparseVector.vectorId
      vectorIdToVector.put(vectorId, vector.sparseVector)
      for (tableId <- tableIds if tableId != -1) {
        vectorDatabase(tableId).put(vector.sparseVector.vectorId, true)
      }
      val currentTime = System.nanoTime()
      if (startTime.containsKey(vectorId)) {
        writeCost += vectorId -> (currentTime - writingTimeMoment)
        endTime += vectorId -> currentTime
      }
    }
  }

  private def sendPerformanceReport(): Unit = {
    val result = new mutable.HashMap[Int, Long]
    for ((vectorId, endMoment) <- endTime if startTime.containsKey(vectorId)) {
      val cost = endMoment - startTime(vectorId)
      if (cost > 0) {
        result += vectorId -> cost
      }
    }
    //get max, min, average
    var overallTuple: (Long, Long, Long) = null
    if (result.nonEmpty) {
      val max = result.maxBy(_._2)
      val min = result.minBy(_._2)
      //get average
      val sum = result.map(_._2).sum
      val average = sum * 1.0 / result.size
      overallTuple = (max._2, min._2, average.toLong)
    }
    //search cost
    var searchTuple: (Long, Long, Long) = null
    if (searchCost.nonEmpty) {
      val max = searchCost.maxBy(_._2)
      val min = searchCost.minBy(_._2)
      //get average
      val sum = searchCost.map(_._2).sum
      val average = sum * 1.0 / result.size
      searchTuple = (max._2, min._2, average.toLong)
    }
    //write cost
    var writeTuple: (Long, Long, Long) = null
    if (writeCost.nonEmpty) {
      val max = writeCost.maxBy(_._2)
      val min = writeCost.minBy(_._2)
      //get average
      val sum = writeCost.map(_._2).sum
      val average = sum * 1.0 / result.size
      writeTuple = (max._2, min._2, average.toLong)
    }
    clientActor ! PerformanceReport(overallTuple, searchTuple, writeTuple)
  }

  override def receive: Receive = {
    case searchRequest @ SearchRequest(_) =>
      processSearchRequest(searchRequest)
    case shardAllocation: FlatShardAllocation =>
      processShardAllocation(shardAllocation)
    case ReceiveTimeout =>
      sendPerformanceReport()
      context.stop(self)
  }
}

private[deploy] object ShardDatabaseWorker {
  val shardDatabaseWorkerActorName = "ShardDatabaseWorkerActor"
  def props(conf: Config, lsh: LSH): Props = Props(new ShardDatabaseWorker(conf, lsh))

  //benchmark stuffs
  private val startTime = new ConcurrentHashMap[Int, Long]
  private val endTime = new ConcurrentHashMap[Int, Long]
  private val searchCost = new ConcurrentHashMap[Int, Long]
  private val writeCost = new ConcurrentHashMap[Int, Long]
}
