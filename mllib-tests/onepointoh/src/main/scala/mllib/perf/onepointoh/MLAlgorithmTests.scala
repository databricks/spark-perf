package mllib.perf.onepointoh

import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy
import org.apache.spark.mllib.tree.impurity.{Gini, Variance}
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD

import mllib.perf.onepointoh.util.DataGenerator

/** Parent class for tests which run on a large dataset. */
abstract class RegressionAndClassificationTests[M](sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[LabeledPoint]): M

  def validate(model: M, rdd: RDD[LabeledPoint]): Double

  val NUM_EXAMPLES =
    ("num-examples", "number of examples for regression and classification tests")
  val NUM_FEATURES =
    ("num-features", "number of features of each example for regression and classification tests")

  intOptions = intOptions ++ Seq(NUM_FEATURES)
  longOptions = longOptions ++ Seq(NUM_EXAMPLES)

  var rdd: RDD[LabeledPoint] = _
  var testRdd: RDD[LabeledPoint] = _

  override def run(): JValue = {
    var start = System.currentTimeMillis()
    val model = runTest(rdd)
    val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    start = System.currentTimeMillis()
    val trainingMetric = validate(model, rdd)
    val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    val testMetric = validate(model, testRdd)
    Map("trainingTime" -> trainingTime, "testTime" -> testTime,
      "trainingMetric" -> trainingMetric, "testMetric" -> testMetric)
  }

  /**
   * For classification
   * @param predictions RDD over (prediction, truth) for each instance
   * @return Percent correctly classified
   */
  def calculateAccuracy(predictions: RDD[(Double, Double)], numExamples: Long): Double = {
    predictions.map{case (pred, label) =>
      if (pred == label) 1.0 else 0.0
    }.sum() * 100.0 / numExamples
  }

  /**
   * For regression
   * @param predictions RDD over (prediction, truth) for each instance
   * @return Root mean squared error (RMSE)
   */
  def calculateRMSE(predictions: RDD[(Double, Double)], numExamples: Long): Double = {
    val error = predictions.map{ case (pred, label) =>
      (pred-label) * (pred-label)
    }.sum()
    math.sqrt(error / numExamples)
  }
}

/** Parent class for Generalized Linear Model (GLM) tests */
abstract class GLMTests(sc: SparkContext)
  extends RegressionAndClassificationTests[GeneralizedLinearModel](sc) {

  val STEP_SIZE =     ("step-size",   "step size for SGD")
  val NUM_ITERATIONS =      ("num-iterations",   "number of iterations for the algorithm")
  val REG_TYPE =      ("reg-type",   "type of regularization: none, l1, l2")
  val REG_PARAM =      ("reg-param",   "the regularization parameter against overfitting")

  intOptions = intOptions ++ Seq(NUM_ITERATIONS)
  doubleOptions = doubleOptions ++ Seq(STEP_SIZE, REG_PARAM)
  stringOptions = stringOptions ++ Seq(REG_TYPE)
}

class GLMRegressionTest(sc: SparkContext) extends GLMTests(sc) {

  val INTERCEPT =  ("intercept",   "intercept for random data generation")
  val EPS =  ("epsilon",   "scale factor for the noise during data generation")
  val LOSS =  ("loss",   "loss to minimize. Supported: l2 (squared error).")

  doubleOptions = doubleOptions ++ Seq(INTERCEPT, EPS)
  stringOptions = stringOptions ++ Seq(LOSS)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    
    val intercept: Double = doubleOptionValue(INTERCEPT)
    val eps: Double = doubleOptionValue(EPS)

    val data = DataGenerator.generateLabeledPoints(sc, math.ceil(numExamples * 1.25).toLong,
      numFeatures, intercept, eps, numPartitions, seed)

    val split = data.randomSplit(Array(0.8, 0.2), seed)

    rdd = split(0).cache()
    testRdd = split(1)

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  override def validate(model: GeneralizedLinearModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()
    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    calculateRMSE(predictions, numExamples)
  }

  override def runTest(rdd: RDD[LabeledPoint]): GeneralizedLinearModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)
    val loss = stringOptionValue(LOSS)
    val regType = stringOptionValue(REG_TYPE)
    val regParam = doubleOptionValue(REG_PARAM)
    val numIterations = intOptionValue(NUM_ITERATIONS)

    if (!Array("l2").contains(loss)) {
      throw new IllegalArgumentException(
        s"GLMRegressionTest run with unknown loss ($loss).  Supported values: l2.")
    }
    if (!Array("none", "l1", "l2").contains(regType)) {
      throw new IllegalArgumentException(
        s"GLMRegressionTest run with unknown regType ($regType).  Supported values: none, l1, l2.")
    }

    (loss, regType) match {
      case ("l2", "none") =>
        val lr = new LinearRegressionWithSGD().setIntercept(addIntercept = true)
        lr.optimizer.setNumIterations(numIterations).setStepSize(stepSize)
        lr.run(rdd)
      case ("l2", "l1") =>
        val lasso = new LassoWithSGD().setIntercept(addIntercept = true)
        lasso.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setRegParam(regParam)
        lasso.run(rdd)
      case ("l2", "l2") =>
        val rr = new RidgeRegressionWithSGD().setIntercept(addIntercept = true)
        rr.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setRegParam(regParam)
        rr.run(rdd)
    }
  }
}

class GLMClassificationTest(sc: SparkContext) extends GLMTests(sc) {

  val THRESHOLD =  ("per-negative",   "probability for a negative label during data generation")
  val SCALE =  ("scale-factor",   "scale factor for the noise during data generation")
  val LOSS =  ("loss",   "loss to minimize. Supported: logistic, hinge (SVM).")

  doubleOptions = doubleOptions ++ Seq(THRESHOLD, SCALE)
  stringOptions = stringOptions ++ Seq(LOSS)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def validate(model: GeneralizedLinearModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()
    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    calculateAccuracy(predictions, numExamples)
  }

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

  override def runTest(rdd: RDD[LabeledPoint]): GeneralizedLinearModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)
    val loss = stringOptionValue(LOSS)
    val regType = stringOptionValue(REG_TYPE)
    val regParam = doubleOptionValue(REG_PARAM)
    val numIterations = intOptionValue(NUM_ITERATIONS)

    if (!Array("logistic", "hinge").contains(loss)) {
      throw new IllegalArgumentException(
        s"GLMClassificationTest run with unknown loss ($loss).  Supported values: logistic, hinge.")
    }
    if (!Array("none", "l1", "l2").contains(regType)) {
      throw new IllegalArgumentException(s"GLMClassificationTest run with unknown regType" +
        s" ($regType).  Supported values: none, l1, l2.")
    }

    (loss, regType) match {
      case ("logistic", "none") =>
        LogisticRegressionWithSGD.train(rdd, numIterations, stepSize)
      case ("hinge", "l2") =>
        SVMWithSGD.train(rdd, numIterations, stepSize, regParam)
      case _ =>
        throw new IllegalArgumentException(
          s"GLMClassificationTest given incompatible (loss, regType) = ($loss, $regType)." +
          s" Note the set of supported combinations increases in later Spark versions.")
    }
  }
}

abstract class RecommendationTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[Rating]): MatrixFactorizationModel

  val NUM_USERS =    ("num-users",   "number of users for recommendation tests")
  val NUM_PRODUCTS = ("num-products", "number of features of each example for recommendation tests")
  val NUM_RATINGS =  ("num-ratings",   "number of ratings for recommendation tests")
  val RANK =         ("rank", "rank of factorized matrices for recommendation tests")
  val IMPLICIT =     ("implicit-prefs", "use implicit ratings")
  val NUM_ITERATIONS =  ("num-iterations",   "number of iterations for the algorithm")
  val REG_PARAM =      ("reg-param",   "the regularization parameter against overfitting")

  intOptions = intOptions ++ Seq(NUM_USERS, NUM_PRODUCTS, RANK, NUM_ITERATIONS)
  longOptions = longOptions ++ Seq(NUM_RATINGS)
  booleanOptions = booleanOptions ++ Seq(IMPLICIT)
  doubleOptions = doubleOptions ++ Seq(REG_PARAM)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var rdd: RDD[Rating] = _
  var testRdd: RDD[Rating] = _

  override def createInputData(seed: Long) = {
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    
    val numUsers: Int = intOptionValue(NUM_USERS)
    val numProducts: Int = intOptionValue(NUM_PRODUCTS)
    val numRatings: Long = longOptionValue(NUM_RATINGS)
    val implicitRatings: Boolean = booleanOptionValue(IMPLICIT)

    val data = DataGenerator.generateRatings(sc, numUsers, numProducts,
      math.ceil(numRatings * 1.25).toLong, implicitRatings,numPartitions,seed)

    rdd = data._1.cache()
    testRdd = data._2

    // Materialize rdd
    println("Num Examples: " + rdd.count())

  }

  def validate(model: MatrixFactorizationModel,
               data: RDD[Rating]): Double = {
    val implicitPrefs: Boolean = booleanOptionValue(IMPLICIT)
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings: RDD[(Double, Double)] = predictions.map{ x =>
      def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values

    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  override def run(): JValue = {
    var start = System.currentTimeMillis()
    val model = runTest(rdd)
    val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    start = System.currentTimeMillis()
    val trainingMetric = validate(model, rdd)
    val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    val testMetric = validate(model, testRdd)
    Map("trainingTime" -> trainingTime, "testTime" -> testTime,
      "trainingMetric" -> trainingMetric, "testMetric" -> testMetric)
  }
}

abstract class ClusteringTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[Vector]): KMeansModel

  val NUM_POINTS =    ("num-points",   "number of points for clustering tests")
  val NUM_COLUMNS =   ("num-columns",   "number of columns for each point for clustering tests")
  val NUM_CENTERS =   ("num-centers",   "number of centers for clustering tests")
  val NUM_ITERATIONS =      ("num-iterations",   "number of iterations for the algorithm")

  intOptions = intOptions ++ Seq(NUM_CENTERS, NUM_COLUMNS, NUM_ITERATIONS)
  longOptions = longOptions ++ Seq(NUM_POINTS)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var rdd: RDD[Vector] = _
  var testRdd: RDD[Vector] = _

  def validate(model: KMeansModel, rdd: RDD[Vector]): Double = {
    val numPoints = rdd.cache().count()

    val error = model.computeCost(rdd)

    math.sqrt(error/numPoints)
  }

  override def createInputData(seed: Long) = {
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    
    val numPoints: Long = longOptionValue(NUM_POINTS)
    val numColumns: Int = intOptionValue(NUM_COLUMNS)
    val numCenters: Int = intOptionValue(NUM_CENTERS)

    val data = DataGenerator.generateKMeansVectors(sc, math.ceil(numPoints*1.25).toLong, numColumns,
      numCenters, numPartitions, seed)

    val split = data.randomSplit(Array(0.8, 0.2), seed)

    rdd = split(0).cache()
    testRdd = split(1)

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  override def run(): JValue = {
    var start = System.currentTimeMillis()
    val model = runTest(rdd)
    val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    start = System.currentTimeMillis()
    val trainingMetric = validate(model, rdd)
    val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    val testMetric = validate(model, testRdd)
    Map("trainingTime" -> trainingTime, "testTime" -> testTime,
      "trainingMetric" -> trainingMetric, "testMetric" -> testMetric)
  }
}

// Classification Algorithms

class NaiveBayesTest(sc: SparkContext)
  extends RegressionAndClassificationTests[NaiveBayesModel](sc) {

  val THRESHOLD =  ("per-negative",   "probability for a negative label during data generation")
  val SCALE =  ("scale-factor",   "scale factor for the noise during data generation")
  val SMOOTHING =     ("nb-lambda",   "the smoothing parameter lambda for Naive Bayes")

  doubleOptions = doubleOptions ++ Seq(THRESHOLD, SCALE, SMOOTHING)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  /** Note: using same data generation as for GLMClassificationTest, but should change later */
  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    val threshold: Double = doubleOptionValue(THRESHOLD)
    val sf: Double = doubleOptionValue(SCALE)

    val data = DataGenerator.generateClassificationLabeledPoints(sc,
      math.ceil(numExamples * 1.25).toLong, numFeatures, threshold, sf, numPartitions, seed)
    val dataNonneg = data.map { lp =>
      LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map(math.abs)))
    }

    val split = dataNonneg.randomSplit(Array(0.8, 0.2), seed)

    rdd = split(0).cache()
    testRdd = split(1)

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  override def validate(model: NaiveBayesModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()
    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    calculateAccuracy(predictions, numExamples)
  }

  override def runTest(rdd: RDD[LabeledPoint]): NaiveBayesModel = {
    val lambda = doubleOptionValue(SMOOTHING)
    NaiveBayes.train(rdd, lambda)
  }
}

// Recommendation
class ALSTest(sc: SparkContext) extends RecommendationTests(sc) {
  override def runTest(rdd: RDD[Rating]): MatrixFactorizationModel = {
    val numIterations: Int = intOptionValue(NUM_ITERATIONS)
    val rank: Int = intOptionValue(RANK)
    val regParam = doubleOptionValue(REG_PARAM)
    val seed = intOptionValue(RANDOM_SEED) + 12

    new ALS().setIterations(numIterations).setRank(rank).setSeed(seed).setLambda(regParam).run(rdd)
  }
}

// Clustering
class KMeansTest(sc: SparkContext) extends ClusteringTests(sc) {
  override def runTest(rdd: RDD[Vector]): KMeansModel = {
    val numIterations: Int = intOptionValue(NUM_ITERATIONS)
    val k: Int = intOptionValue(NUM_CENTERS)
    KMeans.train(rdd, k, numIterations)
  }
}

/**
 * Parent class for DecisionTree-based tests which run on a large dataset.
 */
abstract class DecisionTreeTests(sc: SparkContext)
  extends RegressionAndClassificationTests[DecisionTreeModel](sc) {

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

  intOptions = intOptions ++ Seq(LABEL_TYPE, TREE_DEPTH, MAX_BINS)
  doubleOptions = doubleOptions ++ Seq(FRAC_CATEGORICAL_FEATURES, FRAC_BINARY_FEATURES)

  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  var categoricalFeaturesInfo: Map[Int, Int] = Map.empty

  def validate(model: DecisionTreeModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()
    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    val labelType: Int = intOptionValue(LABEL_TYPE)
    if (labelType == 0) {
      calculateRMSE(predictions, numExamples)
    } else {
      val thresholdedPredictions = predictions.map { case (pred, truth) =>
        val pred01 = if (pred > 0.5) 1.0 else 0.0 // only needed for Spark 1.0, not later versions
        (pred01, truth)
      }
      calculateAccuracy(thresholdedPredictions, numExamples)
    }
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
      DataGenerator.generateDecisionTreeLabeledPoints(sc, math.ceil(numExamples * 1.25).toLong,
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
      DecisionTree.train(rdd, Regression, Variance, treeDepth, maxBins, QuantileStrategy.Sort,
        categoricalFeaturesInfo)
    } else if (labelType == 2) {
      // Classification
      DecisionTree.train(rdd, Classification, Gini, treeDepth,
        maxBins, QuantileStrategy.Sort, categoricalFeaturesInfo)
    } else {
      throw new IllegalArgumentException(
        s"Bad label-type parameter given to DecisionTreeTest: $labelType." +
        s"  Only 0 (regression) or 2 (binary classification) supported for Spark 1.0.")
    }
  }
}
