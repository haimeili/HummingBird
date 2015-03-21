package cpslab.deploy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import akka.actor.{Actor, Props}
import com.typesafe.config.Config
import cpslab.lsh.vector.{SimilarityCalculator, SparseVector}
import cpslab.storage.{ByteArrayWrapper, LongBitSet}

private[deploy] class ShardDatabaseStorage(conf: Config) extends Actor {

  // data structures for different sharding schema
  private[deploy] lazy val elementsInIndependentSpace =
    new mutable.HashMap[ByteArrayWrapper, ListBuffer[(Int, SparseVector)]]
  private[deploy] lazy val elementsInFlatSpace =
    new mutable.HashMap[Int,
      mutable.HashMap[ByteArrayWrapper, ListBuffer[(Int, SparseVector)]]]
  
  private lazy val similarityThreshold = conf.getDouble("cpslab.lsh.similarityThreshold")
  private val shardingNamespace = conf.getString("cpslab.lsh.sharding.namespace")
  private lazy val topK = conf.getInt("cpslab.lsh.topK")

  private val writeActorsNum = conf.getInt("cpslab.lsh.writerActorNum")
  
  private def generateSimilarityOutput(
      vectorID: Int,
      vector: SparseVector,
      candidateList: Option[ListBuffer[(Int, SparseVector)]]):
      Option[SimilarityIntermediateOutput] = {
    if (!candidateList.isDefined) {
      Some(SimilarityIntermediateOutput(vectorID, new LongBitSet, List[(Int, Double)]()))
    } else {
      //calculate similarity
      val selectedCandidates = candidateList.map(allCandidates =>
        allCandidates.foldLeft(new ListBuffer[(Int, Double)])
          ((selectedCandidates, candidate) => selectedCandidates +=
            candidate._1 -> SimilarityCalculator.fastCalculateSimilarity(vector,
              candidate._2))
      )
      selectedCandidates.map(candidates => {
        val result = candidates.filter(_._2 > similarityThreshold).sortWith((a, b) => a._2 > b._2).
          take(topK).toList
        val bitmap = new LongBitSet
        result.foreach(pair => bitmap.set(pair._1))
        SimilarityIntermediateOutput(vectorID, bitmap, result)
      }
      )
    }
  }

  private def handleIndexRequest(indexRequest: LSHTableIndexRequest): Unit = {
    for ((tableId, vectors) <- indexRequest.indexMap; vector <- vectors) {
      val bucketIndex = ByteArrayWrapper(
        vector.bucketIndex.filter(indexInTable => indexInTable != null)(0))
      val allVectorCandidates = shardingNamespace match {
        case "independent" =>
          elementsInIndependentSpace.get(bucketIndex)
        case "flat" =>
          elementsInFlatSpace.getOrElseUpdate(tableId,
            new mutable.HashMap[ByteArrayWrapper, ListBuffer[(Int, SparseVector)]])
          elementsInFlatSpace(tableId).get(bucketIndex)
      }
      val simOutputOpt = generateSimilarityOutput(
        vectorID = vector.vectorID,
        vector = vector.sparseVector,
        candidateList = allVectorCandidates)

      simOutputOpt.foreach(simOutput => {
          val outputActor = context.actorSelection(
            s"/user/writerActor-${simOutput.queryVectorID % writeActorsNum}")
          outputActor ! simOutput
        }
      )
      // index the vector
      writeActorToTable(tableId, bucketIndex, vector.vectorID, vector.sparseVector)
    }
  }

  private def writeActorToTable(
      tableId: Int,
      bucketIndex: ByteArrayWrapper,
      vectorID: Int,
      vector: SparseVector): Unit = {
    shardingNamespace match {
      case "independent" =>
        elementsInIndependentSpace.getOrElseUpdate(bucketIndex,
          new ListBuffer[(Int, SparseVector)]) += (vectorID -> vector)
      case "flat" =>
        elementsInFlatSpace(tableId).getOrElseUpdate(bucketIndex,
          new ListBuffer[(Int, SparseVector)]) += (vectorID -> vector)
    }
  }
  
  override def receive: Receive = {
    case indexRequest @ LSHTableIndexRequest(_) =>
      handleIndexRequest(indexRequest)
  }
}

private[deploy] object ShardDatabaseStorage {
  def props(conf: Config): Props = Props(new ShardDatabaseStorage(conf))
}
