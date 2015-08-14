package mllib.perf.fpm

import org.apache.commons.math3.distribution.BinomialDistribution
import org.apache.commons.math3.random.Well19937c
import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

import mllib.perf.PerfTest

class FPGrowthTest(sc: SparkContext) extends PerfTest {

  val NUM_BASKETS = ("num-baskets", "number of baskets")
  val AVG_BASKET_SIZE = ("avg-basket-size", "average basket size. " +
    "The distribution of basket sizes follows binomial distribution with B(10n,1/10).")
  val NUM_ITEMS = ("num-items", "number of distinct items")
  val MIN_SUPPORT = ("min-support", "minimum support level")

  intOptions = intOptions ++ Seq(NUM_BASKETS, AVG_BASKET_SIZE, NUM_ITEMS)
  doubleOptions = doubleOptions ++ Seq(MIN_SUPPORT)

  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  private var baskets: RDD[Array[Int]] = _

  override def createInputData(seed: Long): Unit = {
    val numPartitions = intOptionValue(NUM_PARTITIONS)
    val numBaskets = intOptionValue(NUM_BASKETS)
    val numItems = intOptionValue(NUM_ITEMS)
    val avgBasketSize = intOptionValue(AVG_BASKET_SIZE)
    val maxRatio = 10
    baskets = sc.parallelize(0 until numBaskets, numPartitions)
      .mapPartitionsWithIndex { (idx, part) =>
        val rng = new Well19937c(seed ^ idx)
        val binom = new BinomialDistribution(rng, maxRatio * avgBasketSize, 1.0 / maxRatio)
        part.map { i =>
          val basketSize = binom.sample()
          // Use math.pow to create a skewed item distribution.
          val items = Array.fill(basketSize)((numItems * math.pow(rng.nextDouble(), 0.1)).toInt)
          items.toSet[Int].toArray // dedup
        }.filter(_.nonEmpty)
      }.cache()
    val exactNumBaskets = baskets.count()
    logInfo(s"Number of baskets: $exactNumBaskets.")
    val totalNumItems = baskets.map(_.length.toLong).reduce(_ + _)
    logInfo(s"Total number of items: $totalNumItems.")
    logInfo(s"Average basket size: ${totalNumItems.toDouble/exactNumBaskets}.")
  }

  override def run(): JValue = {
    val start = System.currentTimeMillis()
    val model = new FPGrowth()
      .setMinSupport(doubleOptionValue(MIN_SUPPORT))
      .setNumPartitions(baskets.partitions.length * 8)
      .run(baskets)
    val numFreqItemsets = model.freqItemsets.count()
    val duration = (System.currentTimeMillis() - start) / 1000.0
    logInfo(s"Number of frequent itemsets: $numFreqItemsets.")
    "time" -> duration
  }
}
