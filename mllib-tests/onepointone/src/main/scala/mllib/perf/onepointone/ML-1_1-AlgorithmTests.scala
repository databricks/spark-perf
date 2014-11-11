package mllib.perf.onepointone

import mllib.perf.onepointone.util.DataGenerator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.{RandomForest, DecisionTree}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.WeightedEnsembleModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

abstract class ClassificationTest[M](sc: SparkContext)
  extends PerfTest {

  def runTest(rdd: RDD[LabeledPoint], numIterations: Int): M
  def validate(model: M, rdd: RDD[LabeledPoint]): Double

  val THRESHOLD = ("per-negative", "probability for a negative label during data generation")
  val SCALE = ("scale-factor", "scale factor for the noise during data generation")
  val SMOOTHING = ("nb-lambda", "the smoothing parameter lambda for Naive Bayes")
  val NUM_EXAMPLES =  ("num-examples",   "number of examples for regression tests")
  val NUM_FEATURES =  ("num-features",   "number of features of each example for regression tests")
  val STEP_SIZE =     ("step-size",   "step size for SGD")

  intOptions = intOptions ++ Seq(NUM_FEATURES)
  longOptions = Seq(NUM_EXAMPLES)
  doubleOptions = doubleOptions ++ Seq(STEP_SIZE,THRESHOLD, SCALE, SMOOTHING)

  var rdd: RDD[LabeledPoint] = _
  var testRdd: RDD[LabeledPoint] = _

  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    val threshold: Double = doubleOptionValue(THRESHOLD)
    val sf: Double = doubleOptionValue(SCALE)

    val data = DataGenerator.generateClassificationLabeledPoints(sc,
      math.ceil(numExamples * 1.25).toLong, numFeatures, threshold, sf, numPartitions, seed)

    val split = data.randomSplit(Array(0.8, 0.2), seed)

    rdd = split(0).cache()
    testRdd = split(1)

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  def calculateAccuracy(predictions: RDD[(Double, Double)], numExamples: Long): Double = {
    predictions.map{case (pred, label) =>
      if (pred == label) 1.0 else 0.0
    }.sum() * 100.0 / numExamples
  }

  override def run(): (Double, Double, Double) = {
    val numIterations = intOptionValue(NUM_ITERATIONS)

    val start = System.currentTimeMillis()
    val model = runTest(rdd, numIterations)
    val end = System.currentTimeMillis()
    val time = (end - start).toDouble / 1000.0
    val metricOnTrain = validate(model, rdd)
    val metric = validate(model, testRdd)

    (time, metricOnTrain, metric)
  }
}

// Classification Algorithms
class LogisticRegressionWithLBFGSTest(sc: SparkContext)
  extends ClassificationTest[LogisticRegressionModel](sc) {

  override val options = intOptions ++ stringOptions  ++ booleanOptions ++
    doubleOptions ++ longOptions

  addOptionsToParser()

  override def validate(model: LogisticRegressionModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()

    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    calculateAccuracy(predictions, numExamples)
  }

  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): LogisticRegressionModel = {

    new LogisticRegressionWithLBFGS().run(rdd)
  }
}

/**
 * Parent class for tests which run on a large dataset.
 *
 * This class is specific to [[org.apache.spark.mllib.tree.DecisionTree]].
 * It should eventually be generalized and merged with
 * RegressionAndClassificationTests
 *
 */
abstract class DecisionTreeTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[LabeledPoint]): WeightedEnsembleModel

  val TEST_DATA_FRACTION =
    ("test-data-fraction",  "fraction of data to hold out for testing (ignored if given training and test dataset)")
  val NUM_EXAMPLES = ("num-examples", "number of examples for regression tests")
  val NUM_FEATURES = ("num-features", "number of features of each example for regression tests")
  val LABEL_TYPE =
    ("label-type", "Type of label: 0 indicates regression, 2+ indicates " +
      "classification with this many classes")
  val FRAC_CATEGORICAL_FEATURES = ("frac-categorical-features",
    "Fraction of features which are categorical")
  val FRAC_BINARY_FEATURES =
    ("frac-binary-features", "Fraction of categorical features which are binary. " +
      "Others have 20 categories.")
  val TREE_DEPTH = ("tree-depth", "Depth of true decision tree model used to label examples.")
  val MAX_BINS = ("max-bins", "Maximum number of bins for the decision tree learning algorithm.")
  val NUM_TREES = ("num-trees", "Number of trees to train.  If 1, run DecisionTree.  If >1, run RandomForest.")
  val FEATURE_SUBSET_STRATEGY =
    ("feature-subset-strategy", "Strategy for feature subset sampling. Supported: auto, all, sqrt, log2, onethird.")

  intOptions = intOptions ++ Seq(NUM_FEATURES, LABEL_TYPE, TREE_DEPTH, MAX_BINS, NUM_TREES)
  longOptions = longOptions ++ Seq(NUM_EXAMPLES)
  doubleOptions = doubleOptions ++ Seq(TEST_DATA_FRACTION, FRAC_CATEGORICAL_FEATURES, FRAC_BINARY_FEATURES)
  stringOptions = stringOptions ++ Seq(FEATURE_SUBSET_STRATEGY)

  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()
  addOptionalOptionToParser("training-data", "path to training dataset (if not given, use random data)", "", classOf[String])
  addOptionalOptionToParser("test-data", "path to test dataset (only used if training dataset given)" +
      " (if not given, hold out part of training data for validation)", "", classOf[String])

  var rdd: RDD[LabeledPoint] = _
  var testRdd: RDD[LabeledPoint] = _
  var categoricalFeaturesInfo: Map[Int, Int] = Map.empty

  protected var labelType = -1

  def computeRMSE(model: WeightedEnsembleModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()

    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    val error = predictions.map { case (pred, label) =>
      (pred - label) * (pred - label)
    }.sum()

    math.sqrt(error / numExamples)
  }

  // When we have a general Model in the new API, we won't need these anymore. We can just move both
  // to PerfTest
  def computeAccuracy(model: WeightedEnsembleModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()

    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    predictions.map { case (pred, label) =>
      if (pred == label) 1.0 else 0.0
    }.sum() * 100.0 / numExamples
  }

  def validate(model: WeightedEnsembleModel, rdd: RDD[LabeledPoint]): Double = {
    rdd.cache()
    if (labelType == 0) {
      computeRMSE(model, rdd)
    } else {
      computeAccuracy(model, rdd)
    }
  }

  override def run(): (Double, Double, Double) = {

    val start = System.currentTimeMillis()
    val model = runTest(rdd)
    val end = System.currentTimeMillis()
    println("Learned model:\n" + model)
    val time = (end - start).toDouble / 1000.0
    val trainError = validate(model, rdd)
    val testError = validate(model, testRdd)

    (time, trainError, testError)
  }
}

// For DecisionTreeTest: PartitionLabelStats tracks the stats for each partition.
class PartitionLabelStats(var min: Double, var max: Double, var distinct: Long, var nonInteger: Boolean)
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

  def labelCombOp(labelStatsA: PartitionLabelStats, labelStatsB: PartitionLabelStats): PartitionLabelStats = {
    labelStatsA.min = Math.min(labelStatsA.min, labelStatsB.min)
    labelStatsA.max = Math.max(labelStatsA.max, labelStatsB.max)
    labelStatsA.distinct = Math.max(labelStatsB.distinct, labelStatsB.distinct)
    labelStatsA
  }
}

class DecisionTreeTest(sc: SparkContext) extends DecisionTreeTests(sc) {

  def getTestDataFraction: Double = {
    val testDataFraction: Double = doubleOptionValue(TEST_DATA_FRACTION)
    assert(testDataFraction >= 0 && testDataFraction <= 1, s"Bad testDataFraction: $testDataFraction")
    testDataFraction
  }

  /** Infer label type from data */
  private def isClassification(data: RDD[LabeledPoint]): Boolean = {
    val labelStats =
      data.mapPartitions(PartitionLabelStats.labelSeqOp)
        .fold(new PartitionLabelStats(Double.MaxValue, Double.MinValue, 0, false))(PartitionLabelStats.labelCombOp)
    labelStats.distinct <= PartitionLabelStats.MAX_CATEGORIES && !labelStats.nonInteger
  }

  /**
   * Load training and test LibSVM-format data files.
   * @return (trainTestDatasets, categoricalFeaturesInfo, numClasses) where
   *          trainTestDatasets = Array(trainingData, testData),
   *          categoricalFeaturesInfo is a map of categorical feature arities, and
   *          numClasses = number of classes label can take.
   */
  private def loadLibSVMFiles(sc: SparkContext, seed: Long): (Array[RDD[LabeledPoint]], Map[Int, Int], Int) = {
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    val trainingDataPath: String = optionValue[String]("training-data")
    val testDataPath: String = optionValue[String]("test-data")
    val testDataFraction: Double = getTestDataFraction
    val trainingData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, trainingDataPath, -1, numPartitions)

    val (rdds, categoricalFeaturesInfo_) = if (testDataPath == "") {
      // randomly split trainingData into train, test
      val splits = trainingData.randomSplit(Array(1.0 - testDataFraction, testDataFraction), seed)
      (splits, Map.empty[Int, Int])
    } else {
      // load test data
      val numFeatures = trainingData.take(1)(0).features.size
      val testData: RDD[LabeledPoint] = MLUtils.loadLibSVMFile(sc, testDataPath, numFeatures, numPartitions)
      (Array(trainingData, testData), Map.empty[Int, Int])
    }

    // For classification, re-index classes if needed.
    val (finalDatasets, classIndexMap, numClasses) = if (isClassification(rdds(0)) && isClassification(rdds(1))) {
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

    (finalDatasets, categoricalFeaturesInfo_, numClasses)
  }

  override def createInputData(seed: Long) = {
    val trainingDataPath: String = optionValue[String]("training-data")
    val (rdds, categoricalFeaturesInfo_, numClasses) = if (trainingDataPath != "") {
      println(s"LOADING FILE: $trainingDataPath")
      loadLibSVMFiles(sc, seed)
    } else {
      createSyntheticInputData(seed)
    }
    assert(rdds.length == 2)
    rdd = rdds(0).cache()
    testRdd = rdds(1)
    categoricalFeaturesInfo = categoricalFeaturesInfo_
    this.labelType = numClasses

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  /**
   * Create synthetic training and test datasets.
   * @return (trainTestDatasets, categoricalFeaturesInfo, numClasses) where
   *          trainTestDatasets = Array(trainingData, testData),
   *          categoricalFeaturesInfo is a map of categorical feature arities, and
   *          numClasses = number of classes label can take.
   */
  private def createSyntheticInputData(seed: Long): (Array[RDD[LabeledPoint]], Map[Int, Int], Int) = {
    // Generic test options
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    val testDataFraction: Double = getTestDataFraction
    // Data dimensions and type
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val labelType: Int = intOptionValue(LABEL_TYPE)
    val fracCategoricalFeatures: Double = doubleOptionValue(FRAC_CATEGORICAL_FEATURES)
    val fracBinaryFeatures: Double = doubleOptionValue(FRAC_BINARY_FEATURES)
    // Model specification
    val treeDepth: Int = intOptionValue(TREE_DEPTH)

    val (rdd_, categoricalFeaturesInfo_) =
      DataGenerator.generateDecisionTreeLabeledPoints(sc, math.ceil(numExamples*1.25).toLong,
        numFeatures, numPartitions, labelType,
        fracCategoricalFeatures, fracBinaryFeatures, treeDepth, seed)

    val splits = rdd_.randomSplit(Array(1.0 - testDataFraction, testDataFraction), seed)
    (splits, categoricalFeaturesInfo_, labelType)
  }

  override def runTest(rdd: RDD[LabeledPoint]): WeightedEnsembleModel = {
    val treeDepth: Int = intOptionValue(TREE_DEPTH)
    val maxBins: Int = intOptionValue(MAX_BINS)
    val numTrees: Int = intOptionValue(NUM_TREES)
    val featureSubsetStrategy: String = stringOptionValue(FEATURE_SUBSET_STRATEGY)
    if (labelType == 0) {
      // Regression
      RandomForest.trainRegressor(rdd, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, "variance",
        treeDepth, maxBins, this.getRandomSeed)
    } else if (labelType >= 2) {
      // Classification
      RandomForest.trainClassifier(rdd, labelType, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, "gini",
        treeDepth, maxBins, this.getRandomSeed)
    } else {
      throw new IllegalArgumentException(s"Bad label-type parameter " +
        s"given to DecisionTreeTest: $labelType")
    }
  }
}
