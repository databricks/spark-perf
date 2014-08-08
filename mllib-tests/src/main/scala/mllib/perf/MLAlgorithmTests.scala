package mllib.perf


import mllib.perf.util.DataGenerator
import org.apache.spark.mllib.optimization.{SquaredL2Updater, LeastSquaresGradient, LBFGS}

import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/** Parent class for tests which run on a large dataset. */
abstract class RegressionAndClassificationTests[M](sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[LabeledPoint], numIterations: Int): M

  def validate(model: M, rdd: RDD[LabeledPoint]): Double

  val NUM_EXAMPLES =  ("num-examples",   "number of examples for regression tests")
  val NUM_FEATURES =  ("num-features",   "number of features of each example for regression tests")
  val STEP_SIZE =     ("step-size",   "step size for SGD")

  intOptions = intOptions ++ Seq(NUM_FEATURES)
  longOptions = Seq(NUM_EXAMPLES)
  doubleOptions = doubleOptions ++ Seq(STEP_SIZE)

  var rdd: RDD[LabeledPoint] = _
  var testRdd: RDD[LabeledPoint] = _

}

abstract class RegressionTest(sc: SparkContext) extends RegressionAndClassificationTests[GeneralizedLinearModel](sc) {

  val INTERCEPT =  ("intercept",   "intercept for random data generation")
  val EPS =  ("epsilon",   "scale factor for the noise during data generation")

  doubleOptions = doubleOptions ++ Seq(INTERCEPT, EPS)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()
  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    
    val intercept: Double = doubleOptionValue(INTERCEPT)
    val eps: Double = doubleOptionValue(EPS)

    val data = DataGenerator.generateLabeledPoints(sc, math.ceil(numExamples*1.25).toLong,
      numFeatures, intercept, eps, numPartitions,seed)

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
    val error = predictions.map{case (pred, label) =>
      (pred-label) * (pred-label)
    }.reduce(_ + _)

    math.sqrt(error / numExamples)
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

abstract class ClassificationTest[M](sc: SparkContext) extends RegressionAndClassificationTests[M](sc) {

  val THRESHOLD =  ("per-negative",   "probability for a negative label during data generation")
  val SCALE =  ("scale-factor",   "scale factor for the noise during data generation")
  val SMOOTHING =     ("nb-lambda",   "the smoothing parameter lambda for Naive Bayes")

  doubleOptions = doubleOptions ++ Seq(THRESHOLD, SCALE, SMOOTHING)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
    
    val threshold: Double = doubleOptionValue(THRESHOLD)
    val sf: Double = doubleOptionValue(SCALE)

    val data = DataGenerator.generateClassificationLabeledPoints(sc, math.ceil(numExamples*1.25).toLong,
      numFeatures, threshold, sf, numPartitions,seed)

    val split = data.randomSplit(Array(0.8, 0.2), seed)

    rdd = split(0).cache()
    testRdd = split(1)

    // Materialize rdd
    println("Num Examples: " + rdd.count())
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

  def calculateAccuracy(predictions: RDD[(Double, Double)], numExamples: Long): Double = {
    predictions.map{case (pred, label) =>
      pred.toByte ^ label.toByte ^ 1
    }.reduce(_ + _) * 100.0 / numExamples
  }

}

/**
 * Parent class for tests which run on a large dataset.
 *
 * This class is specific to [[org.apache.spark.mllib.tree.DecisionTree]].
 * It should eventually be generalized and merged with [[mllib.perf.RegressionAndClassificationTests]]
 */
abstract class DecisionTreeTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[LabeledPoint]): DecisionTreeModel

  val NUM_EXAMPLES = ("num-examples", "number of examples for regression tests")
  val NUM_FEATURES = ("num-features", "number of features of each example for regression tests")
  val LABEL_TYPE =
    ("label-type", "Type of label: 0 indicates regression, 2+ indicates classification with this many classes")
  val FRAC_CATEGORICAL_FEATURES = ("frac-categorical-features", "Fraction of features which are categorical")
  val FRAC_BINARY_FEATURES =
    ("frac-binary-features", "Fraction of categorical features which are binary. Others have 20 categories.")
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
    }.reduce(_ + _)

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
      pred.toByte ^ label.toByte ^ 1
    }.reduce(_ + _) * 100.0 / numExamples
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

abstract class RecommendationTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[Rating], numIterations: Int, rank: Int): MatrixFactorizationModel

  val NUM_USERS =     ("num-users",   "number of users for recommendation tests")
  val NUM_PRODUCTS =  ("num-products", "number of features of each example for recommendation tests")
  val NUM_RATINGS =   ("num-ratings",   "number of ratings for recommendation tests")
  val RANK =          ("rank", "rank of factorized matrices for recommendation tests")
  val IMPLICIT =      ("implicit-prefs", "use implicit ratings")

  intOptions = intOptions ++ Seq(NUM_USERS, NUM_PRODUCTS, RANK)
  longOptions = longOptions ++ Seq(NUM_RATINGS)
  booleanOptions = booleanOptions ++ Seq(IMPLICIT)
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

    val data = DataGenerator.generateRatings(sc, numUsers, numProducts, math.ceil(numRatings*1.25).toLong,
      implicitRatings,numPartitions,seed)

    rdd = data._1.cache()
    testRdd = data._2

    // Materialize rdd
    println("Num Examples: " + rdd.count())

  }

  def validate(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings: RDD[(Double, Double)] = predictions.map{ x =>
      def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values

    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  override def run(): (Double, Double, Double) = {
    val numIterations: Int = intOptionValue(NUM_ITERATIONS)
    val rank: Int = intOptionValue(RANK)
    val implicitRatings: Boolean = booleanOptionValue(IMPLICIT)

    val start = System.currentTimeMillis()
    val model = runTest(rdd, numIterations, rank)
    val end = System.currentTimeMillis()
    val time = (end - start).toDouble / 1000.0

    val trainError = validate(model, rdd,implicitRatings)
    val testError = validate(model, testRdd,implicitRatings)
    (time, trainError, testError)

  }
}

abstract class ClusteringTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[Vector], numIterations: Int, numCenters: Int): KMeansModel

  val NUM_POINTS =    ("num-points",   "number of points for clustering tests")
  val NUM_COLUMNS =   ("num-columns",   "number of columns for each point for clustering tests")
  val NUM_CENTERS =   ("num-centers",   "number of centers for clustering tests")

  intOptions = intOptions ++ Seq(NUM_CENTERS, NUM_COLUMNS)
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

  override def run(): (Double, Double, Double) = {
    val numIterations: Int = intOptionValue(NUM_ITERATIONS)
    val k: Int = intOptionValue(NUM_CENTERS)

    val start = System.currentTimeMillis()
    val model = runTest(rdd, numIterations, k)
    val end = System.currentTimeMillis()
    val time = (end - start).toDouble / 1000.0
    val trainError = validate(model, rdd)
    val testError = validate(model, testRdd)

    (time, trainError, testError)
  }
}

// Regression Algorithms
class LinearRegressionTest(sc: SparkContext) extends RegressionTest(sc) {
  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): LinearRegressionModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)
    val lr = new LinearRegressionWithSGD().setIntercept(true)
    lr.optimizer.setNumIterations(numIterations).setStepSize(stepSize)

    lr.run(rdd)
  }
}

class RidgeRegressionTest(sc: SparkContext) extends RegressionTest(sc) {
  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): RidgeRegressionModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)
    val regParam = doubleOptionValue(REGULARIZATION)
    val rr = new RidgeRegressionWithSGD().setIntercept(true)
    rr.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setRegParam(regParam)

    rr.run(rdd)
  }
}

// The default optimizer is SGD, therefore a little more work is required to run L-BFGS
class RidgeRegressionWithLBFGSTest(sc: SparkContext) extends RegressionTest(sc) {

  val CONVERGENCE =  ("convergence-tol",   "convergence tolerance for l-bfgs")
  val CORRECTIONS =  ("num-corrections",   "number of corrections for l-bfgs")

  doubleOptions = doubleOptions ++ Seq(CONVERGENCE)
  intOptions = intOptions ++ Seq(CORRECTIONS)

  override val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): RidgeRegressionModel = {

    val regParam = doubleOptionValue(REGULARIZATION)
    val tol = doubleOptionValue(CONVERGENCE)
    val numCor = intOptionValue(CORRECTIONS)
    val numFeatures = intOptionValue(NUM_FEATURES)
    val addIntercept = doubleOptionValue(INTERCEPT) != 0.0
    val initialWeights = Vectors.dense(Array.fill(numFeatures)(0.0))

    val lbfgs = new LBFGS(new LeastSquaresGradient(), new SquaredL2Updater())
    lbfgs.setMaxNumIterations(numIterations).setRegParam(regParam).setConvergenceTol(tol).setNumCorrections(numCor)

    // Prepend an extra variable consisting of all 1.0's for the intercept.
    val data = if (addIntercept) {
      rdd.map(labeledPoint => (labeledPoint.label, appendBias(labeledPoint.features)))
    } else {
      rdd.map(labeledPoint => (labeledPoint.label, labeledPoint.features))
    }

    val initialWeightsWithIntercept = if (addIntercept) {
      appendBias(initialWeights)
    } else {
      initialWeights
    }

    val weightsWithIntercept = lbfgs.optimize(data, initialWeightsWithIntercept)

    val intercept = if (addIntercept) weightsWithIntercept(weightsWithIntercept.size - 1) else 0.0
    val weights =
      if (addIntercept) {
        Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1))
      } else {
        weightsWithIntercept
      }

    new RidgeRegressionModel(weights, intercept)
  }
}

class LassoTest(sc: SparkContext) extends RegressionTest(sc) {
  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): LassoModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)
    val regParam = doubleOptionValue(REGULARIZATION)
    val lasso = new LassoWithSGD().setIntercept(true)
    lasso.optimizer.setNumIterations(numIterations).setStepSize(stepSize).setRegParam(regParam)

    lasso.run(rdd)
  }
}

// Classification Algorithms
class LogisticRegressionTest(sc: SparkContext) extends ClassificationTest[LogisticRegressionModel](sc) {

  override def validate(model: LogisticRegressionModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()

    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    calculateAccuracy(predictions, numExamples)
  }

  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): LogisticRegressionModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)

    LogisticRegressionWithSGD.train(rdd, numIterations, stepSize)
  }
}

class NaiveBayesTest(sc: SparkContext) extends ClassificationTest[NaiveBayesModel](sc) {

  override def validate(model: NaiveBayesModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()

    val predictions: RDD[(Double, Double)] = rdd.map { example =>
      (model.predict(example.features), example.label)
    }
    calculateAccuracy(predictions, numExamples)
  }

  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): NaiveBayesModel = {
    val lambda = doubleOptionValue(SMOOTHING)

    NaiveBayes.train(rdd, lambda)
  }
}

class SVMTest(sc: SparkContext) extends ClassificationTest[SVMModel](sc) {

  override def validate(model: SVMModel, rdd: RDD[LabeledPoint]): Double = {
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
    
    val sf: Double = doubleOptionValue(SCALE)

    val data = DataGenerator.generateLabeledPoints(sc, math.ceil(numExamples*1.25).toLong,
      numFeatures, 0.0, sf, numPartitions,seed, "SVM")

    val split = data.randomSplit(Array(0.8, 0.2), seed)

    rdd = split(0).cache()
    testRdd = split(1)

    // Materialize rdd
    println("Num Examples: " + rdd.count())
  }

  override def runTest(rdd: RDD[LabeledPoint], numIterations: Int): SVMModel = {
    val stepSize = doubleOptionValue(STEP_SIZE)
    val regParam = doubleOptionValue(REGULARIZATION)

    SVMWithSGD.train(rdd, numIterations, stepSize, regParam)
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
      DataGenerator.generateDecisionTreeLabeledPoints(sc, math.ceil(numExamples*1.25).toLong, numFeatures, numPartitions,
        labelType, fracCategoricalFeatures, fracBinaryFeatures, treeDepth, seed)

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
      DecisionTree.train(rdd, Classification, Gini, treeDepth, labelType, maxBins, QuantileStrategy.Sort,
        categoricalFeaturesInfo)
    } else {
      throw new IllegalArgumentException(s"Bad label-type parameter given to DecisionTreeTest: $labelType")
    }
  }
}


// Recommendation
class ALSTest(sc: SparkContext) extends RecommendationTests(sc) {
  override def runTest(rdd: RDD[Rating], numIterations: Int, rank: Int): MatrixFactorizationModel = {
    val regParam = doubleOptionValue(REGULARIZATION)

    ALS.train(rdd, rank, numIterations, regParam)
  }
}

// Clustering
class KMeansTest(sc: SparkContext) extends ClusteringTests(sc) {
  override def runTest(rdd: RDD[Vector], numIterations: Int, numCenters: Int): KMeansModel = {
    KMeans.train(rdd, numCenters, numIterations)
  }
}





