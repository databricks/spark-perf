package mllib.perf.clustering

import mllib.perf.PerfTest

import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.collection.mutable.{HashMap => MHashMap}

import org.apache.commons.math3.random.Well19937c
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

class LDATest(sc: SparkContext) extends PerfTest {

  val NUM_DOCUMENTS = ("num-documents", "number of documents in corpus")
  val NUM_VOCABULARY = ("num-vocab", "number of terms in vocabulary")
  val NUM_TOPICS = ("num-topics", "number of topics to infer")
  val NUM_ITERATIONS = ("num-iterations", "number of iterations for the algorithm")
  val DOCUMENT_LENGTH = ("document-length", "number of words per document for the algorithm")
  val OPTIMIZER = ("optimizer", "optimization algorithm: em or online")

  intOptions ++= Seq(NUM_VOCABULARY, NUM_TOPICS, NUM_ITERATIONS, DOCUMENT_LENGTH)
  longOptions ++= Seq(NUM_DOCUMENTS)
  stringOptions ++= Seq(OPTIMIZER)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var data: RDD[(Long, Vector)] = _

  override def createInputData(seed: Long): Unit = {
    val numDocs = longOptionValue(NUM_DOCUMENTS)
    val numVocab = intOptionValue(NUM_VOCABULARY)
    val k = intOptionValue(NUM_TOPICS)

    val numPartitions = intOptionValue(NUM_PARTITIONS)
    val docLength = intOptionValue(DOCUMENT_LENGTH)

    data = sc.parallelize(0L until numDocs, numPartitions)
      .mapPartitionsWithIndex { (idx, part) =>
      val rng = new Well19937c(seed ^ idx)
      part.map { case docIndex =>
        var currentSize = 0
        val entries = MHashMap[Int, Int]()
        while (currentSize < docLength) {
          val index = rng.nextInt(numVocab)
          entries(index) = entries.getOrElse(index, 0) + 1
          currentSize += 1
        }

        val iter = entries.toSeq.map(v => (v._1, v._2.toDouble))
        (docIndex, Vectors.sparse(numVocab, iter))
      }
    }.cache()
    logInfo(s"Number of documents = ${data.count()}.")
  }

  override def run(): JValue = {
    val k = intOptionValue(NUM_TOPICS)
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val optimizer = stringOptionValue(OPTIMIZER)
    val start = System.currentTimeMillis()
    val lda = new LDA()
      .setK(k)
      .setMaxIterations(numIterations)
      .setOptimizer(optimizer)
    val model = lda.run(data)
    val duration = (System.currentTimeMillis() - start) / 1e3
    "time" -> duration
  }
}
