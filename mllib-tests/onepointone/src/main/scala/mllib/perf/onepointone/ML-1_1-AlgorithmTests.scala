package mllib.perf.onepointone

import mllib.perf.onepointone.util.DataGenerator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.DecisionTreeModel
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

  def runTest(rdd: RDD[LabeledPoint]): DecisionTreeModel

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

  intOptions = intOptions ++ Seq(NUM_FEATURES, LABEL_TYPE, TREE_DEPTH, MAX_BINS)
  longOptions = longOptions ++ Seq(NUM_EXAMPLES)

  doubleOptions = doubleOptions ++ Seq(FRAC_CATEGORICAL_FEATURES, FRAC_BINARY_FEATURES)

  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  var rdd: RDD[LabeledPoint] = _
  var testRdd: RDD[LabeledPoint] = _
  var categoricalFeaturesInfo: Map[Int, Int] = Map.empty

  def computeRMSE(model: DecisionTreeModel, rdd: RDD[LabeledPoint]): Double = {
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
  def computeAccuracy(model: DecisionTreeModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()

    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    predictions.map { case (pred, label) =>
      if (pred == label) 1.0 else 0.0
    }.sum() * 100.0 / numExamples
  }

  def validate(model: DecisionTreeModel, rdd: RDD[LabeledPoint]): Double = {
    val labelType: Int = intOptionValue(LABEL_TYPE)
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
    val time = (end - start).toDouble / 1000.0
    val trainError = validate(model, rdd)
    val testError = validate(model, testRdd)

    (time, trainError, testError)
  }
}

class DecisionTreeTest(sc: SparkContext) extends DecisionTreeTests(sc) {

  override def createInputData(seed: Long) = {
    // Generic test options
    
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
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

    val splits = rdd_.randomSplit(Array(0.8, 0.2), seed)

    rdd = splits(0).cache()
    testRdd = splits(1)
    categoricalFeaturesInfo = categoricalFeaturesInfo_

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  override def runTest(rdd: RDD[LabeledPoint]): DecisionTreeModel = {
    val labelType: Int = intOptionValue(LABEL_TYPE)
    val treeDepth: Int = intOptionValue(TREE_DEPTH)
    val maxBins: Int = intOptionValue(MAX_BINS)
    if (labelType == 0) {
      // Regression
      DecisionTree.train(rdd, Regression, Variance, treeDepth, 0, maxBins, QuantileStrategy.Sort,
        categoricalFeaturesInfo)
    } else if (labelType >= 2) {
      // Classification
      DecisionTree.train(rdd, Classification, Gini, treeDepth, labelType,
        maxBins, QuantileStrategy.Sort, categoricalFeaturesInfo)
    } else {
      throw new IllegalArgumentException(s"Bad label-type parameter " +
        s"given to DecisionTreeTest: $labelType")
    }
  }
}
