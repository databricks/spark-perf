package mllib.perf.clustering

import mllib.perf.PerfTest
import org.apache.commons.math3.random.Well19937c
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.collection.mutable.{ArrayBuilder => MArrayBuilder, HashMap => MHashMap}

class LDATest(sc: SparkContext) extends PerfTest {

  val Num_DOCUMENTS = ("num-documents", "number of documents in corpus")
  val Num_VOCABULARY = ("num-vocab", "number of terms in vocabulary")
  val NUM_TOPICS = ("num-topics", "number of topics to infer")
  val NUM_ITERATIONS = ("num-iterations", "number of iterations for the algorithm")

  intOptions ++= Seq(Num_VOCABULARY, NUM_TOPICS, NUM_ITERATIONS)
  longOptions ++= Seq(Num_DOCUMENTS)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var data: RDD[(Long, Vector)] = _

  override def createInputData(seed: Long): Unit = {

    val numDocs = longOptionValue(Num_DOCUMENTS)
    val numVocab = intOptionValue(Num_VOCABULARY)
    val k = intOptionValue(NUM_TOPICS)

    val numPartitions = intOptionValue(NUM_PARTITIONS)
    val maxDocLength = 100
    val maxOccurence = 5 // max occurence of a term in one document

    val data = sc.parallelize(0L until numDocs, numPartitions)
      .mapPartitionsWithIndex { (idx, part) =>
      val rng = new Well19937c(seed ^ idx)
      val nnz = math.min(numVocab, maxDocLength)
      part.map { case docIndex =>
        val entries = MHashMap[Int, Int]()
        while (entries.size < nnz) {
          entries += ((rng.nextInt(numVocab), rng.nextInt(maxOccurence)))
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
    val start = System.currentTimeMillis()
    val lda = new LDA()
      .setK(k)
      .setMaxIterations(numIterations)
    val model = lda.run(data)
    val duration = (System.currentTimeMillis() - start) / 1e3
    println(duration)
    "time" -> duration
  }


}
