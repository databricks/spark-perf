package mllib.perf.util

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object DataLoader {

  // For DecisionTreeTest: PartitionLabelStats tracks the stats for each partition.
  class PartitionLabelStats(
      var min: Double,
      var max: Double,
      var distinct: Long,
      var nonInteger: Boolean)
    extends Serializable

  object PartitionLabelStats extends Serializable {
    /** Max categories allowed for categorical label (for inferring labelType) */
    val MAX_CATEGORIES = 1000

    def labelSeqOp(lps: Iterator[LabeledPoint]): Iterator[PartitionLabelStats] = {
      val stats = new PartitionLabelStats(Double.MaxValue, Double.MinValue, 0, false)
      val labelSet = new scala.collection.mutable.HashSet[Double]()
      lps.foreach { lp =>
        if (lp.label.toInt != lp.label) {
          stats.nonInteger = true
        }
        stats.min = Math.min(lp.label, stats.min)
        stats.max = Math.max(lp.label, stats.max)
        if (labelSet.size <= MAX_CATEGORIES) {
          labelSet.add(lp.label)
        }
      }
      stats.distinct = labelSet.size
      Iterator(stats)
      Iterator(new PartitionLabelStats(0,0,0,false))
    }

    def labelCombOp(
        labelStatsA: PartitionLabelStats,
        labelStatsB: PartitionLabelStats): PartitionLabelStats = {
      labelStatsA.min = Math.min(labelStatsA.min, labelStatsB.min)
      labelStatsA.max = Math.max(labelStatsA.max, labelStatsB.max)
      labelStatsA.distinct = Math.max(labelStatsB.distinct, labelStatsB.distinct)
      labelStatsA
    }
  }

  /** Infer label type from data */
  private def isClassification(data: RDD[LabeledPoint]): Boolean = {
    val labelStats =
      data.mapPartitions(PartitionLabelStats.labelSeqOp)
        .fold(new PartitionLabelStats(Double.MaxValue, Double.MinValue, 0, false))(
          PartitionLabelStats.labelCombOp)
    labelStats.distinct <= PartitionLabelStats.MAX_CATEGORIES && !labelStats.nonInteger
  }

  /**
   * Load training and test LibSVM-format data files.
   * @return (trainTestDatasets, categoricalFeaturesInfo, numClasses) where
   *         trainTestDatasets = Array(trainingData, testData),
   *         categoricalFeaturesInfo is a map of categorical feature arities, and
   *         numClasses = number of classes label can take.
   */
  private[perf] def loadLibSVMFiles(
      sc: SparkContext,
      numPartitions: Int,
      trainingDataPath: String,
      testDataPath: String,
      testDataFraction: Double,
      seed: Long): (Array[RDD[LabeledPoint]], Map[Int, Int], Int) = {

    val trainingData = MLUtils.loadLibSVMFile(sc, trainingDataPath, -1, numPartitions)

    val (rdds, categoricalFeaturesInfo_) = if (testDataPath == "") {
      // randomly split trainingData into train, test
      val splits = trainingData.randomSplit(Array(1.0 - testDataFraction, testDataFraction), seed)
      (splits, Map.empty[Int, Int])
    } else {
      // load test data
      val numFeatures = trainingData.take(1)(0).features.size
      val testData = MLUtils.loadLibSVMFile(sc, testDataPath, numFeatures, numPartitions)
      (Array(trainingData, testData), Map.empty[Int, Int])
    }

    // For classification, re-index classes if needed.
    val (finalDatasets, classIndexMap, numClasses) = {
      if (isClassification(rdds(0)) && isClassification(rdds(1))) {
        // classCounts: class --> # examples in class
        val classCounts: Map[Double, Long] = {
          val trainClassCounts = rdds(0).map(_.label).countByValue()
          val testClassCounts = rdds(1).map(_.label).countByValue()
          val mutableClassCounts = new scala.collection.mutable.HashMap[Double, Long]()
          trainClassCounts.foreach { case (label, cnt) =>
            mutableClassCounts(label) = mutableClassCounts.getOrElseUpdate(label, 0) + cnt
          }
          testClassCounts.foreach { case (label, cnt) =>
            mutableClassCounts(label) = mutableClassCounts.getOrElseUpdate(label, 0) + cnt
          }
          mutableClassCounts.toMap
        }
        val sortedClasses = classCounts.keys.toList.sorted
        val numClasses = classCounts.size
        // classIndexMap: class --> index in 0,...,numClasses-1
        val classIndexMap = {
          if (classCounts.keySet != Set(0.0, 1.0)) {
            sortedClasses.zipWithIndex.toMap
          } else {
            Map[Double, Int]()
          }
        }
        val indexedRdds = {
          if (classIndexMap.isEmpty) {
            rdds
          } else {
            rdds.map { rdd =>
              rdd.map(lp => LabeledPoint(classIndexMap(lp.label), lp.features))
            }
          }
        }
        val numTrain = indexedRdds(0).count()
        val numTest = indexedRdds(1).count()
        val numTotalInstances = numTrain + numTest
        println(s"numTrain: $numTrain")
        println(s"numTest: $numTest")
        println(s"numClasses: $numClasses")
        println(s"Per-class example fractions, counts:")
        println(s"Class\tFrac\tCount")
        sortedClasses.foreach { c =>
          val frac = classCounts(c) / numTotalInstances.toDouble
          println(s"$c\t$frac\t${classCounts(c)}")
        }
        (indexedRdds, classIndexMap, numClasses)
      } else {
        (rdds, null, 0)
      }
    }

    (finalDatasets, categoricalFeaturesInfo_, numClasses)
  }

}
