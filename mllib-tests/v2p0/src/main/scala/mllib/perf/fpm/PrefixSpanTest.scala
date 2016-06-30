package mllib.perf.fpm

import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.commons.math3.random.Well19937c
import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.rdd.RDD

import mllib.perf.PerfTest

class PrefixSpanTest(sc: SparkContext) extends PerfTest {

  val NUM_SEQUENCES = ("num-sequences", "number of itemset sequences")
  val AVG_SEQUENCE_SIZE = ("avg-sequence-size", "average number of itemsets in a sequence. " +
    "The distribution of itemset sequence sizes follows binomial distribution with B(10n,1/10).")
  val AVG_ITEMSET_SIZE = ("avg-itemset-size", "average number of items in a itemset. " +
    "The distribution of itemset sizes follows binomial distribution with B(10n,1/10).")
  val NUM_ITEMS = ("num-items", "number of distinct items")
  val MIN_SUPPORT = ("min-support", "minimum support level")
  val MAX_PATTERN_LEN = ("max-pattern-len", "maximum length of frequent itemset sequences")
  val MAX_LOCAL_PROJ_DB_SIZE = ("max-local-proj-db-size", "maximum number of items allowed in a " +
    "locally processed projected database")

  intOptions ++= Seq(NUM_SEQUENCES, AVG_SEQUENCE_SIZE, AVG_ITEMSET_SIZE, NUM_ITEMS,
    MAX_PATTERN_LEN, MAX_LOCAL_PROJ_DB_SIZE)
  doubleOptions ++= Seq(MIN_SUPPORT)
  longOptions ++= Seq(MAX_LOCAL_PROJ_DB_SIZE)


  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  private var sequences: RDD[Array[Array[Int]]] = _

  override def createInputData(seed: Long): Unit = {
    val numPartitions = intOptionValue(NUM_PARTITIONS)
    val numSequences = intOptionValue(NUM_SEQUENCES)
    val numItems = intOptionValue(NUM_ITEMS)
    val avgSequenceSize = intOptionValue(AVG_SEQUENCE_SIZE)
    val avgItemsetSize = intOptionValue(AVG_ITEMSET_SIZE)
    val maxRatio = 10
    sequences = sc.parallelize(0 until numSequences, numPartitions)
      .mapPartitionsWithIndex { (idx, part) =>
      val rng = new Well19937c(seed ^ idx)
      val binomSeq = new BinomialDistribution(rng, maxRatio * avgSequenceSize, 1.0 / maxRatio)
      val binomItemset = new BinomialDistribution(rng, maxRatio * avgItemsetSize, 1.0 / maxRatio)
      part.map { i =>
        val seqSize = binomSeq.sample()
        // Use math.pow to create a skewed item distribution.
        val items = Array.fill(seqSize)(
          Array.fill(binomItemset.sample())((numItems * math.pow(rng.nextDouble(), 0.1)).toInt)
        )
        items.map(_.toSet[Int].toArray) // dedup
      }.filter(_.nonEmpty)
    }.cache()
    val exactNumSeqs = sequences.count()
    logInfo(s"Number of sequences: $exactNumSeqs.")
    val totalNumItems = sequences.map(_.flatten.length.toLong).reduce(_ + _)
    val totalNumItemsets = sequences.map(_.length.toLong).reduce(_ + _)
    logInfo(s"Total number of items: $totalNumItems.")
    logInfo(s"Total number of itemsets: $totalNumItemsets.")
    logInfo(s"Average num itemsets per sequence: ${totalNumItemsets.toDouble/exactNumSeqs}.")
    logInfo(s"Average num items per itemset: ${totalNumItems.toDouble/totalNumItemsets}.")
  }

  override def run(): JValue = {
    val start = System.currentTimeMillis()
    val model = new PrefixSpan()
      .setMinSupport(doubleOptionValue(MIN_SUPPORT))
      .setMaxPatternLength(intOptionValue(MAX_PATTERN_LEN))
      .setMaxLocalProjDBSize(longOptionValue(MAX_LOCAL_PROJ_DB_SIZE))
      .run(sequences)
    val numFreqItemsets = model.freqSequences.count()
    val duration = (System.currentTimeMillis() - start) / 1000.0
    logInfo(s"Number of frequent sequences: $numFreqItemsets.")
    "time" -> duration
  }
}

