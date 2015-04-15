package cpslab.deploy.plsh

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.typesafe.config.Config
import cpslab.deploy.{SearchRequest, SimilarityIntermediateOutput, SimilaritySearchMessages, Utils}
import cpslab.lsh._
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}

private[plsh] class PLSHWorker(id: Int, conf: Config, lshInstance: LSH) extends Actor {

  // general setup
  private val tableNum = conf.getInt("cpslab.lsh.tableNum")
  private val updateWindowSize = conf.getInt("cpslab.lsh.plsh.updateWindowSize")
  private var withinUpdateWindow = false
  private val mergeThreshold = conf.getLong("cpslab.lsh.plsh.mergeThreshold")
  private val maxNumberOfVector = conf.getLong("cpslab.lsh.plsh.maxNumberOfVector")
  private[plsh] val elementCountInDeltaTable = new AtomicLong(0)
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")
  private lazy val inputFilePath = conf.getString("cpslab.lsh.inputFilePath")
  private val logger = Logging(context.system, this)

  // vector storage
  //two-level partition
  // 2d array
  // tableID => (vectorID, bucketIndex)
  private[plsh] var twoLevelPartitionTable =
    Array.fill[Array[(Int, Int)]](tableNum)(null)
  private[plsh] var bucketOffsetTable: Array[mutable.HashMap[Int, Int]] =
    Array.fill[mutable.HashMap[Int, Int]](tableNum)(null)
  private[plsh] val deltaTable = Array.fill(tableNum)(new mutable.HashMap[Int,
    ListBuffer[Int]])

  //variables controlling the merge thread
  private val workerThreadCount: AtomicInteger = new AtomicInteger(0)
  private val mergingThreadCount: AtomicInteger = new AtomicInteger(0)

  private var vectorIdToVector: Array[SparseVector] = new Array[SparseVector](0)
  private var totalVectorCount = 0
  private val initVectorNumber = conf.getInt("cpslab.lsh.initVectorNumber")

  //parallel task support
  //private val parallelTaskSupport = new ThreadPoolTaskSupport()

  override def preStart(): Unit = {
    // read files and save to the hash table
    try {
      //initialize the vector
      initVectorStorage(inputFilePath)
      logger.info("Finished loading data from file system ")
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s"Cannot initialize the storage space at $inputFilePath")
    }
  }

  private def initVectorStorage(filePath: String)(implicit executorService: ExecutionContext):
      Unit = {
    if (filePath != "") {
      // read all files
      vectorIdToVector = new Array[SparseVector](initVectorNumber)
      val allFiles = Utils.buildFileListUnderDirectory(filePath)
      for (file <- allFiles; line <- Source.fromFile(file).getLines()) {
        val (size, indices, values, id) = Vectors.fromString(line)
        val vector = new SparseVector(id, size, indices, values)
        vectorIdToVector(vector.vectorId) = vector
        totalVectorCount += 1
      }
    }
    if (vectorIdToVector.length > 0) {
      initTwoLevelPartitionTable()
    }
  }

  /**
   * initialize the twoLevelPartitionTable based on the vector saved in vectorIdToVector
   * NOTE: we need to ensure that at any moment, at most one thread is calling this function
   */
  private def initTwoLevelPartitionTable(): Unit = {
    val startTime = System.nanoTime()
    logger.info("Initializing Static Table ")
    // initialize tables
    for (i <- 0 until tableNum) {
      twoLevelPartitionTable(i) = new Array[(Int, Int)](vectorIdToVector.size)
    }
    val parVectorIdToVector = vectorIdToVector.par
    // calculate the bucket index for all vectors in all tables
    val bucketIndexOfAllVectors: Iterable[Array[(Int, Int)]] =
      parVectorIdToVector.filter(_ != null).map(sparseVector =>
        lshInstance.calculateIndex(sparseVector).map(bucketIndex =>
          (bucketIndex, sparseVector.vectorId))).seq
    // calculate the offset of each bucket in all tables
    bucketOffsetTable = calculateOffsetofAllBuckets(bucketIndexOfAllVectors)
    // calculate offset of each vector
    calculateOffSetForAllVectors(bucketIndexOfAllVectors)
    logger.info(s"Finished Static Table Building, take time ${System.nanoTime() - startTime}")
  }

  /**
   * be called after the offset of each bucket in each table has been calculated
   * @param bucketIndexOfAllVectors the bucket index of the vectors in all tables
   * @return the offset of all bucket index in all tables
   */
  private def calculateOffsetofAllBuckets(
      bucketIndexOfAllVectors: Iterable[Array[(Int, Int)]]):
      Array[mutable.HashMap[Int, Int]] = {
    val tempTable = Array.fill[ConcurrentHashMap[Int, AtomicInteger]](tableNum)(
      new ConcurrentHashMap[Int, AtomicInteger])
    bucketIndexOfAllVectors.par.foreach(indicesOfVectorInAllTables => {
      var tableId = 0
      for (bucketIndexOfVectorInThisTable <- indicesOfVectorInAllTables) {
        tempTable(tableId).getOrElseUpdate(bucketIndexOfVectorInThisTable._1, new AtomicInteger(0))
        tempTable(tableId)(bucketIndexOfVectorInThisTable._1).getAndAdd(1)
        tableId += 1
        if (tableId == tableNum) {
          tableId = 0
        }
      }
    })

    val bucketCountArray = tempTable.map(table => {
      val parTable = table.par
      //group by bucket index
      parTable.map { case (bucketIndex, cnt) => (bucketIndex, cnt.get()) }.seq
    })
    //translate from count to offset
    bucketCountArray.par.map(bucketCountMapPerTable => {
      var currentTotalCnt = 0
      var bucketOffsetPerTable = new mutable.HashMap[Int, Int]
      for ((bucketIndex, cnt) <- bucketCountMapPerTable) {
        bucketOffsetPerTable += bucketIndex -> currentTotalCnt
        currentTotalCnt += cnt
      }
      bucketOffsetPerTable
    }).toArray
  }

  /**
   * called after the offset of the bucket index has been calculated, calculated the offset
   * of all vectors in all tables; twoLevelPartitionTable is updated
   * @param bucketIndexOfAllVectors bucket index of all vectors
   */
  private def calculateOffSetForAllVectors(
      bucketIndexOfAllVectors: Iterable[Array[(Int, Int)]]): Unit = {

    assert(bucketOffsetTable != null)

    val inBucketOffsetArray =
      Array.fill[mutable.HashMap[Int, AtomicInteger]](tableNum)(
        new mutable.HashMap[Int, AtomicInteger])
    //initialize the offset counter for each bucket in all tables
    for (i <- 0 until tableNum) {
      bucketOffsetTable(i).foreach{case (bucketIndex, _) =>
        inBucketOffsetArray(i) += bucketIndex -> new AtomicInteger(0)}
    }
    // update twoLevelPartitionTable with all vectors
    // parallel over vector instances
    val parBucketIndexOfAllVectors = bucketIndexOfAllVectors.par
    //parBucketIndexOfAllVectors.tasksupport = parallelTaskSupport
    parBucketIndexOfAllVectors.foreach(bucketIndicesOfVector => {
      var tableId = 0
      bucketIndicesOfVector.foreach{case (bucketIndex, vectorId) =>
        // compute offset
        val offset = bucketOffsetTable(tableId)(bucketIndex) +
          inBucketOffsetArray(tableId)(bucketIndex).getAndIncrement
        twoLevelPartitionTable(tableId)(offset) = (vectorId, bucketIndex)
        tableId += 1
      }
    })
  }

  /**
   * select the similar vectors from similar candidates based on similarity threshold and topK and
   * send back to the client
   * @param similarCandidates the similar candidates
   * @param queryVector the query vector
   * @param clientAddress the address of client actor
   */
  private def selectAndResponseSimilarCandidates(similarCandidates: util.BitSet,
      queryVector: SparseVector, clientAddress: ActorRef): Unit = {
    if (similarCandidates.cardinality() > 0) {
      var nextSimilarVectorID = similarCandidates.nextSetBit(0)
      val similarVectors = new ListBuffer[(Int, Double)]
      while (nextSimilarVectorID >= 0) {
        //calculate the similarity
        val similarity = SimilarityCalculator.fastCalculateSimilarity(
          vectorIdToVector(nextSimilarVectorID), queryVector)
        if (similarity >= similarityThreshold) {
          similarVectors += nextSimilarVectorID -> similarity
        }
        nextSimilarVectorID = similarCandidates.nextSetBit(nextSimilarVectorID + 1)
      }
      //send to sender
      clientAddress ! SimilarityIntermediateOutput(queryVector.vectorId, null,
        similarVectors.take(topK).toList)
    }
  }

  /**
   * query static and delta table for the similar candidates,
   * @param bucketIndicesOfQuery the bucket indices of the query vectors
   * @return BitSet representing the similar candidates
   */
  private def queryTablesForSimilarCandidates(bucketIndicesOfQuery: Array[Int]):
      util.BitSet = {
    val similarCandidates = new util.BitSet
    for (tableId <- 0 until tableNum) {
      //calculate the query vector offset within the bucket
      val bucketIndex = bucketIndicesOfQuery(tableId)
      //query static table
      // we need to guard to handle the case that the node starts without initialization of
      // anything from the local file system and handle the search request directly
      if (bucketOffsetTable(tableId) != null) {
        val vectorOffsetOpt = bucketOffsetTable(tableId).get(bucketIndex)
        vectorOffsetOpt.foreach(vectorOffset => {
          var vectorOffsetWorkerPointer = vectorOffset
          var candidateVectorAndBucketIndex = twoLevelPartitionTable(tableId)(
            vectorOffsetWorkerPointer)
          while (
              candidateVectorAndBucketIndex != null &&
              candidateVectorAndBucketIndex._2 == bucketIndex &&
              vectorOffsetWorkerPointer < twoLevelPartitionTable(tableId).length
          ) {
            candidateVectorAndBucketIndex = twoLevelPartitionTable(tableId)(
              vectorOffsetWorkerPointer)
            if (candidateVectorAndBucketIndex != null) {
              similarCandidates.set(vectorIdToVector(candidateVectorAndBucketIndex._1).vectorId)
            }
            vectorOffsetWorkerPointer += 1
          }
        })
      }
      //query delta table
      deltaTable(tableId).synchronized {
        val candidatesOpt = deltaTable(tableId).get(bucketIndex)
        candidatesOpt.foreach(candidate =>
          candidate.foreach(candidateInt => similarCandidates.set(candidateInt)))
      }
    }
    similarCandidates
  }

  /**
   * save query vector to the delta table
   * @param queryVector query vector
   * @param bucketIndices the bucket indices of the query vector in all tables
   * @param client the client address to receive notification when the table is full (client should
   *               update the sliding window range)
   */
  private def saveQueryVectorToDeltaTable(
      queryVector: SparseVector,
      bucketIndices: Array[Int],
      client: ActorRef): Unit = {
    elementCountInDeltaTable.getAndIncrement
    totalVectorCount += 1
    for (tableId <- 0 until bucketIndices.length) {
      val bucketIndex = bucketIndices(tableId)
      if (withinUpdateWindow && bucketIndex % updateWindowSize ==
          (id - updateWindowSize * (id / updateWindowSize))) {
        vectorIdToVector.synchronized {
          if (vectorIdToVector.length < totalVectorCount) {
            //TODO: pooling memory space
            vectorIdToVector = vectorIdToVector ++ new Array[SparseVector](16)
          }
          vectorIdToVector(queryVector.vectorId) = queryVector
        }
        deltaTable(tableId).synchronized {
          deltaTable(tableId).getOrElseUpdate(bucketIndex, new ListBuffer[Int]) +=
            queryVector.vectorId
        }
      }
    }
    if (vectorIdToVector.length >= maxNumberOfVector) {
      client ! CapacityFullNotification(id)
    }
    tryToMergeDeltaAndStaticTable()
  }

  /**
   * check if need to merge and delta table
   */
  private def tryToMergeDeltaAndStaticTable(): Unit = {
    import PLSHExecutionContext._
    if (workerThreadCount.get() <= 1 && mergingThreadCount.get() == 0 &&
      elementCountInDeltaTable.get() >= mergeThreshold) {
      mergingThreadCount.incrementAndGet()
      executorService.execute {
        new Runnable() {
          override def run() {
            initVectorStorage("")
            for (i <- 0 until deltaTable.length) {
              deltaTable(i).clear()
            }
            elementCountInDeltaTable.set(0)
          }
        }
      }
      mergingThreadCount.decrementAndGet()
    }
  }

  /**
   * process the search request when the vectors are saved in a table which is partitioned in
   * two levels
   * @param queryVector the query vector
   * @param clientActor the actor in client end
   */
  private def handleSearchRequestWithTwoLevelParittionTable(
      queryVector: SparseVector,
      clientActor: ActorRef): Unit = {
    workerThreadCount.getAndIncrement
    //calculate the bucket indices of the query vector
    val queryIndexInAllTable = lshInstance.calculateIndex(queryVector)
    //query the tabables for the similar candidates
    val similarCandidates = queryTablesForSimilarCandidates(queryIndexInAllTable)
    //select similar candidates and send back to the sender
    selectAndResponseSimilarCandidates(similarCandidates, queryVector, clientActor)
    //save the query vector to delta table
    saveQueryVectorToDeltaTable(queryVector, queryIndexInAllTable, clientActor)
    workerThreadCount.getAndDecrement
  }

  private def handleSearchRequest(vector: SparseVector, clientActor: ActorRef)
      (implicit executorService: ExecutionContext): Unit = {
    executorService.execute(new Runnable {
      override def run(): Unit = {
        val threadLocalSender = new ThreadLocal[ActorRef]
        threadLocalSender.set(clientActor)
        handleSearchRequestWithTwoLevelParittionTable(vector, threadLocalSender.get())
      }
    })
  }

  private def handleSimilaritySearchMessages(similarityMessages: SimilaritySearchMessages): Unit =
    similarityMessages match {
    case SearchRequest(vector: SparseVector) =>
      while (mergingThreadCount.get() > 0) {Thread.sleep(100)}
      handleSearchRequest(vector, sender())
    case other =>
      logger.error(s"unrecognizable message: $other")
  }

  private def handlePLSHMessages(plshMessage: PLSHMessage): Unit = plshMessage match {
    case WindowUpdate(lowerBound, upperBound) =>
      val client = new ThreadLocal[ActorRef]
      client.set(sender())
      if (id >= lowerBound && id <= upperBound) {
        withinUpdateWindow = true
      } else {
        withinUpdateWindow = false
      }
      client.get() ! WindowUpdateNotification(id)
  }

  override def receive: Receive = {
    case simSearchMsg: SimilaritySearchMessages =>
      handleSimilaritySearchMessages(simSearchMsg)
    case plshMsg: PLSHMessage =>
      handlePLSHMessages(plshMsg)
  }
}

private[deploy] object PLSHWorker {
  def props(id: Int, conf: Config, lshInstance: LSH): Props = {
    Props(new PLSHWorker(id, conf, lshInstance))
  }
}
