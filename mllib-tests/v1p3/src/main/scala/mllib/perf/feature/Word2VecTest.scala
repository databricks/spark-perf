package mllib.perf.feature

import scala.collection.mutable

import org.apache.commons.math3.random.Well19937c
import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD

import mllib.perf.PerfTest

class Word2VecTest(sc: SparkContext) extends PerfTest {

  val NUM_SENTENCES = ("num-sentences", "number of sentences")
  val NUM_WORDS = ("num-words", "vocabulary size")
  val VECTOR_SIZE = ("vector-size", "vector size")
  val NUM_ITERATIONS = ("num-iterations", "number of iterations")
  val MIN_COUNT = ("min-count", "minimum count for a word to be included")

  intOptions ++= Seq(NUM_SENTENCES, NUM_WORDS, VECTOR_SIZE, NUM_ITERATIONS, MIN_COUNT)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  private val avgSentenceLength = 16
  private var sentences: RDD[Seq[String]] = _

  override def createInputData(seed: Long): Unit = {
    val numSentences = intOptionValue(NUM_SENTENCES)
    val numPartitions = intOptionValue(NUM_PARTITIONS)
    val numWords = intOptionValue(NUM_WORDS)
    val p = 1.0 / avgSentenceLength
    sentences = sc.parallelize(0 until numSentences, numPartitions)
      .mapPartitionsWithIndex { (idx, part) =>
        val rng = new Well19937c(seed ^ idx)
        part.map { case i =>
          var cur = rng.nextInt(numWords)
          val sentence = mutable.ArrayBuilder.make[Int]
          while (rng.nextDouble() > p) {
            cur = (cur + rng.nextGaussian() * 10).toInt % numWords
            if (cur < 0) {
              cur += numWords
            }
            sentence += cur
          }
          sentence.result().map(_.toString).toSeq
        }
      }.cache()
    logInfo(s"Number of sentences = ${sentences.count()}.")
  }

  override def run(): JValue = {
    val start = System.currentTimeMillis()
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val numPartitions = math.ceil(math.pow(numIterations, 1.5)).toInt
    val w2v = new Word2Vec()
      .setNumPartitions(numPartitions)
      .setNumIterations(numIterations)
      .setVectorSize(intOptionValue(VECTOR_SIZE))
      .setMinCount(intOptionValue(MIN_COUNT))
      .setSeed(0L)
    val model = w2v.fit(sentences)
    val duration = (System.currentTimeMillis() - start) / 1e3
    "time" -> duration
  }
}
