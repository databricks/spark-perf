package mllib.perf

import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.ml.PredictionModel
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier, RandomForestClassificationModel, RandomForestClassifier, LogisticRegression}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor, RandomForestRegressionModel, RandomForestRegressor, LinearRegression}
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater, SimpleUpdater}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.{Algo, BoostingStrategy, QuantileStrategy, Strategy}
import org.apache.spark.mllib.tree.impurity.Variance
import org.apache.spark.mllib.tree.loss.{LogLoss, SquaredError}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

import mllib.perf.util.{DataGenerator, DataLoader}

/** Parent class for tests which run on a large dataset. */
abstract class RegressionAndClassificationTests[M](sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[LabeledPoint]): M

  def validate(model: M, rdd: RDD[LabeledPoint]): Double

  val NUM_EXAMPLES =  ("num-examples",   "number of examples for regression tests")
  val NUM_FEATURES =  ("num-features",   "number of features of each example for regression tests")

  intOptions = intOptions ++ Seq(NUM_FEATURES)
  longOptions = Seq(NUM_EXAMPLES)

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
      (pred - label) * (pred - label)
    }.sum()
    math.sqrt(error / numExamples)
  }
}

/** Parent class for Generalized Linear Model (GLM) tests */
abstract class GLMTests(sc: SparkContext)
  extends RegressionAndClassificationTests[GeneralizedLinearModel](sc) {

  val STEP_SIZE =         ("step-size",   "step size for SGD")
  val NUM_ITERATIONS =    ("num-iterations",   "number of iterations for the algorithm")
  val REG_TYPE =          ("reg-type",   "type of regularization: none, l1, l2, elastic-net")
  val ELASTIC_NET_PARAM = ("elastic-net-param",   "elastic-net param, 0.0 for L2, and 1.0 for L1")
  val REG_PARAM =         ("reg-param",   "the regularization parameter against overfitting")
  val OPTIMIZER =         ("optimizer", "optimization algorithm (elastic-net only supports l-bfgs): sgd, l-bfgs")

  intOptions = intOptions ++ Seq(NUM_ITERATIONS)
  doubleOptions = doubleOptions ++ Seq(ELASTIC_NET_PARAM, STEP_SIZE, REG_PARAM)
  stringOptions = stringOptions ++ Seq(REG_TYPE, OPTIMIZER)
}

class GLMRegressionTest(sc: SparkContext) extends GLMTests(sc) {

  val INTERCEPT =  ("intercept",   "intercept for random data generation")
  val FEATURE_NOISE =  ("feature-noise",
    "scale factor for the noise during feature generation; CURRENTLY IGNORED")
  val LABEL_NOISE =  ("label-noise",   "scale factor for the noise during label generation")
  val LOSS =  ("loss",   "loss to minimize. Supported: l2 (squared error).")

  doubleOptions = doubleOptions ++ Seq(INTERCEPT, FEATURE_NOISE, LABEL_NOISE)
  stringOptions = stringOptions ++ Seq(LOSS)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    val intercept: Double = doubleOptionValue(INTERCEPT)
    val labelNoise: Double = doubleOptionValue(LABEL_NOISE)

    val data = DataGenerator.generateLabeledPoints(sc, math.ceil(numExamples * 1.25).toLong,
      numFeatures, intercept, labelNoise, numPartitions, seed)

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
    val elasticNetParam = doubleOptionValue(ELASTIC_NET_PARAM)
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val optimizer = stringOptionValue(OPTIMIZER)

    // Linear Regression only supports squared loss for now.
    if (!Array("l2").contains(loss)) {
      throw new IllegalArgumentException(
        s"GLMRegressionTest run with unknown loss ($loss).  Supported values: l2.")
    }

    if (regType == "elastic-net") {  // use spark.ml
      assert(optimizer == "auto" || optimizer == "l-bfgs", "GLMClassificationTest with" +
        s" regType=elastic-net expects optimizer to be in {auto, l-bfgs}, but found: $optimizer")
      println("WARNING: Linear Regression with elastic-net in ML package uses LBFGS/OWLQN for" +
        " optimization which ignores stepSize in Spark 1.5.")
      val rr = new LinearRegression()
        .setElasticNetParam(elasticNetParam)
        .setRegParam(regParam)
        .setMaxIter(numIterations)
        .setTol(0.0)
      val sqlContext = new SQLContext(rdd.context)
      import sqlContext.implicits._
      val mlModel = rr.fit(rdd.toDF())
      new LinearRegressionModel(mlModel.weights, mlModel.intercept)
    } else {
      assert(optimizer == "sgd", "GLMClassificationTest with" +
        s" regType!=elastic-net expects optimizer to be sgd, but found: $optimizer")
      (loss, regType) match {
        case ("l2", "none") =>
          val lr = new LinearRegressionWithSGD().setIntercept(addIntercept = true)
          lr.optimizer
            .setNumIterations(numIterations)
            .setStepSize(stepSize)
            .setConvergenceTol(0.0)
          lr.run(rdd)
        case ("l2", "l1") =>
          val lasso = new LassoWithSGD().setIntercept(addIntercept = true)
          lasso.optimizer
            .setNumIterations(numIterations)
            .setStepSize(stepSize)
            .setRegParam(regParam)
            .setConvergenceTol(0.0)
          lasso.run(rdd)
        case ("l2", "l2") =>
          val rr = new RidgeRegressionWithSGD().setIntercept(addIntercept = true)
          rr.optimizer
            .setNumIterations(numIterations)
            .setStepSize(stepSize)
            .setRegParam(regParam)
            .setConvergenceTol(0.0)
          rr.run(rdd)
        case _ =>
          throw new IllegalArgumentException(
            s"GLMRegressionTest given incompatible (loss, regType) = ($loss, $regType)." +
              s" Note the set of supported combinations increases in later Spark versions.")
      }
    }
  }
}

class GLMClassificationTest(sc: SparkContext) extends GLMTests(sc) {

  val THRESHOLD =  ("per-negative",   "probability for a negative label during data generation")
  val FEATURE_NOISE =  ("feature-noise",   "scale factor for the noise during feature generation")
  val LOSS =  ("loss",   "loss to minimize. Supported: logistic, hinge (SVM).")

  doubleOptions = doubleOptions ++ Seq(THRESHOLD, FEATURE_NOISE)
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
    val featureNoise: Double = doubleOptionValue(FEATURE_NOISE)

    val data = DataGenerator.generateClassificationLabeledPoints(sc,
      math.ceil(numExamples * 1.25).toLong, numFeatures, threshold, featureNoise, numPartitions,
      seed)

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
    val elasticNetParam = doubleOptionValue(ELASTIC_NET_PARAM)
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val optimizer = stringOptionValue(OPTIMIZER)

    // For classification problem in GLM, we currently support logistic loss and hinge loss.
    if (!Array("logistic", "hinge").contains(loss)) {
      throw new IllegalArgumentException(
        s"GLMClassificationTest run with unknown loss ($loss).  Supported values: logistic, hinge.")
    }

    if (regType == "elastic-net") {  // use spark.ml
      assert(optimizer == "auto" || optimizer == "l-bfgs", "GLMClassificationTest with" +
        " regType=elastic-net expects optimizer to be in {auto, l-bfgs}")
      loss match {
        case "logistic" =>
          println("WARNING: Logistic Regression with elastic-net in ML package uses LBFGS/OWLQN" +
            " for optimization which ignores stepSize in Spark 1.5.")
          val lor = new LogisticRegression()
            .setElasticNetParam(elasticNetParam)
            .setRegParam(regParam)
            .setMaxIter(numIterations)
            .setTol(0.0)
          val sqlContext = new SQLContext(rdd.context)
          import sqlContext.implicits._
          val mlModel = lor.fit(rdd.toDF())
          new LogisticRegressionModel(mlModel.weights, mlModel.intercept)
        case _ =>
          throw new IllegalArgumentException(
            s"GLMClassificationTest given unsupported loss = $loss." +
              s" Note the set of supported combinations increases in later Spark versions.")
      }
    } else {
      val updater = regType match {
        case "none" => new SimpleUpdater
        case "l1" => new L1Updater
        case "l2" => new SquaredL2Updater
      }
      (loss, optimizer) match {
        case ("logistic", "sgd") =>
          val lr = new LogisticRegressionWithSGD()
          lr.optimizer
            .setStepSize(stepSize)
            .setNumIterations(numIterations)
            .setConvergenceTol(0.0)
            .setUpdater(updater)
          lr.run(rdd)
        case ("logistic", "l-bfgs") =>
          println("WARNING: LogisticRegressionWithLBFGS ignores stepSize in this Spark version.")
          val lr = new LogisticRegressionWithLBFGS()
          lr.optimizer
            .setNumIterations(numIterations)
            .setConvergenceTol(0.0)
            .setUpdater(updater)
          lr.run(rdd)
        case ("hinge", "sgd") =>
          val svm = new SVMWithSGD()
          svm.optimizer
            .setNumIterations(numIterations)
            .setStepSize(stepSize)
            .setRegParam(regParam)
            .setConvergenceTol(0.0)
            .setUpdater(updater)
          svm.run(rdd)
        case _ =>
          throw new IllegalArgumentException(
            s"GLMClassificationTest given incompatible (loss, regType) = ($loss, $regType)." +
              s" Supported combinations include: (elastic-net, _), (logistic, sgd), (logistic, l-bfgs), (hinge, sgd)." +
              s" Note the set of supported combinations increases in later Spark versions.")
      }
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
      numRatings, implicitRatings, numPartitions, seed)

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

    /*
    // Removed temporarily because these methods are really slow.
    val numThingsToRecommend = 10
    start = System.currentTimeMillis()
    model.recommendProductsForUsers(numThingsToRecommend).count()
    val recommendProductsForUsersTime = (System.currentTimeMillis() - start).toDouble / 1000.0
    start = System.currentTimeMillis()
    model.recommendUsersForProducts(numThingsToRecommend).count()
    val recommendUsersForProductsTime = (System.currentTimeMillis() - start).toDouble / 1000.0
    */
    Map("trainingTime" -> trainingTime, "testTime" -> testTime,
      "trainingMetric" -> trainingMetric, "testMetric" -> testMetric)
    // "recommendProductsForUsersTime" -> recommendProductsForUsersTime,
    // "recommendUsersForProductsTime" -> recommendUsersForProductsTime)
  }
}

abstract class ClusteringTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[Vector]): KMeansModel

  val NUM_EXAMPLES =    ("num-examples",   "number of examples for clustering tests")
  val NUM_FEATURES =   ("num-features",  "number of features for each example for clustering tests")
  val NUM_CENTERS =   ("num-centers",   "number of centers for clustering tests")
  val NUM_ITERATIONS =      ("num-iterations",   "number of iterations for the algorithm")

  intOptions = intOptions ++ Seq(NUM_CENTERS, NUM_FEATURES, NUM_ITERATIONS)
  longOptions = longOptions ++ Seq(NUM_EXAMPLES)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var rdd: RDD[Vector] = _
  var testRdd: RDD[Vector] = _

  def validate(model: KMeansModel, rdd: RDD[Vector]): Double = {
    val numExamples = rdd.cache().count()

    val error = model.computeCost(rdd)

    math.sqrt(error/numExamples)
  }

  override def createInputData(seed: Long) = {
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numCenters: Int = intOptionValue(NUM_CENTERS)

    val data = DataGenerator.generateKMeansVectors(sc, math.ceil(numExamples*1.25).toLong, numFeatures,
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
  val FEATURE_NOISE =  ("feature-noise",   "scale factor for the noise during feature generation")
  val SMOOTHING =     ("nb-lambda",   "the smoothing parameter lambda for Naive Bayes")
  val MODEL_TYPE = ("model-type", "either multinomial (default) or bernoulli")

  doubleOptions = doubleOptions ++ Seq(THRESHOLD, FEATURE_NOISE, SMOOTHING)
  stringOptions = stringOptions ++ Seq(MODEL_TYPE)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  /** Note: using same data generation as for GLMClassificationTest, but should change later */
  override def createInputData(seed: Long) = {
    val numExamples: Long = longOptionValue(NUM_EXAMPLES)
    val numFeatures: Int = intOptionValue(NUM_FEATURES)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    val threshold: Double = doubleOptionValue(THRESHOLD)
    val featureNoise: Double = doubleOptionValue(FEATURE_NOISE)
    val modelType = stringOptionValue(MODEL_TYPE)

    val data = if (modelType == "bernoulli") {
      DataGenerator.generateBinaryLabeledPoints(sc,
        math.ceil(numExamples * 1.25).toLong, numFeatures, threshold, numPartitions, seed)
    } else {
      val negdata = DataGenerator.generateClassificationLabeledPoints(sc,
        math.ceil(numExamples * 1.25).toLong, numFeatures, threshold, featureNoise, numPartitions,
        seed)
      val dataNonneg = negdata.map { lp =>
        LabeledPoint(lp.label, Vectors.dense(lp.features.toArray.map(math.abs)))
      }
      dataNonneg
    }

    val split = data.randomSplit(Array(0.8, 0.2), seed)

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

    val modelType = stringOptionValue(MODEL_TYPE)
    NaiveBayes.train(rdd, lambda, modelType)
  }
}


// Recommendation
class ALSTest(sc: SparkContext) extends RecommendationTests(sc) {
  override def runTest(rdd: RDD[Rating]): MatrixFactorizationModel = {
    val numIterations: Int = intOptionValue(NUM_ITERATIONS)
    val rank: Int = intOptionValue(RANK)
    val regParam = doubleOptionValue(REG_PARAM)
    val seed = intOptionValue(RANDOM_SEED) + 12
    val implicitRatings: Boolean = booleanOptionValue(IMPLICIT)

    new ALS().setIterations(numIterations).setRank(rank).setSeed(seed).setLambda(regParam)
      .setBlocks(rdd.partitions.length).setImplicitPrefs(implicitRatings).run(rdd)
  }
}

// Clustering
// TODO: refactor into mllib.perf.clustering like the other clustering tests
class KMeansTest(sc: SparkContext) extends ClusteringTests(sc) {
  override def runTest(rdd: RDD[Vector]): KMeansModel = {
    val numIterations: Int = intOptionValue(NUM_ITERATIONS)
    val k: Int = intOptionValue(NUM_CENTERS)
    KMeans.train(rdd, k, numIterations)
  }
}

// Decision-tree
sealed trait TreeBasedModel
case class MLlibRFModel(model: RandomForestModel) extends TreeBasedModel
case class MLlibGBTModel(model: GradientBoostedTreesModel) extends TreeBasedModel
case class MLRFRegressionModel(model: RandomForestRegressionModel) extends TreeBasedModel
case class MLRFClassificationModel(model: RandomForestClassificationModel) extends TreeBasedModel
case class MLGBTRegressionModel(model: GBTRegressionModel) extends TreeBasedModel
case class MLGBTClassificationModel(model: GBTClassificationModel) extends TreeBasedModel

/**
 * Parent class for DecisionTree-based tests which run on a large dataset.
 */
abstract class DecisionTreeTests(sc: SparkContext)
  extends RegressionAndClassificationTests[TreeBasedModel](sc) {

  val TEST_DATA_FRACTION =
    ("test-data-fraction",  "fraction of data to hold out for testing (ignored if given training and test dataset)")
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
  val NUM_TREES = ("num-trees", "Number of trees to train.  If 1, run DecisionTree.  If >1, run an ensemble method (RandomForest).")
  val FEATURE_SUBSET_STRATEGY =
    ("feature-subset-strategy", "Strategy for feature subset sampling. Supported: auto, all, sqrt, log2, onethird.")

  intOptions = intOptions ++ Seq(LABEL_TYPE, TREE_DEPTH, MAX_BINS, NUM_TREES)
  doubleOptions = doubleOptions ++ Seq(TEST_DATA_FRACTION, FRAC_CATEGORICAL_FEATURES, FRAC_BINARY_FEATURES)
  stringOptions = stringOptions ++ Seq(FEATURE_SUBSET_STRATEGY)

  addOptionalOptionToParser("training-data", "path to training dataset (if not given, use random data)", "", classOf[String])
  addOptionalOptionToParser("test-data", "path to test dataset (only used if training dataset given)" +
      " (if not given, hold out part of training data for validation)", "", classOf[String])

  var categoricalFeaturesInfo: Map[Int, Int] = Map.empty

  protected var labelType = -1

  def validate(model: TreeBasedModel, rdd: RDD[LabeledPoint]): Double = {
    val numExamples = rdd.count()
    val predictions: RDD[(Double, Double)] = model match {
      case MLlibRFModel(rfModel) => rfModel.predict(rdd.map(_.features)).zip(rdd.map(_.label))
      case MLlibGBTModel(gbtModel) => gbtModel.predict(rdd.map(_.features)).zip(rdd.map(_.label))
      case MLRFRegressionModel(rfModel) => makePredictions(rfModel, rdd)
      case MLRFClassificationModel(rfModel) => makePredictions(rfModel, rdd)
      case MLGBTRegressionModel(gbtModel) => makePredictions(gbtModel, rdd)
      case MLGBTClassificationModel(gbtModel) => makePredictions(gbtModel, rdd)
    }
    val labelType: Int = intOptionValue(LABEL_TYPE)
    if (labelType == 0) {
      calculateRMSE(predictions, numExamples)
    } else {
      calculateAccuracy(predictions, numExamples)
    }
  }

  // TODO: generate DataFrame outside of `runTest` so it is not included in timing results
  private def makePredictions(
      model: PredictionModel[Vector, _], rdd: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    val labelType: Int = intOptionValue(LABEL_TYPE)
    val dataFrame = DataGenerator.setMetadata(rdd, categoricalFeaturesInfo, labelType)
    val results = model.transform(dataFrame)
    results
      .select(model.getPredictionCol, model.getLabelCol)
      .map { case Row(prediction: Double, label: Double) => (prediction, label) }
  }
}

class DecisionTreeTest(sc: SparkContext) extends DecisionTreeTests(sc) {
  val supportedTreeTypes = Array("RandomForest", "GradientBoostedTrees",
    "ml.RandomForest", "ml.GradientBoostedTrees")

  val ENSEMBLE_TYPE = ("ensemble-type", "Type of ensemble algorithm: " + supportedTreeTypes.mkString(" "))

  stringOptions = stringOptions ++ Seq(ENSEMBLE_TYPE)

  val options = intOptions ++ stringOptions ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  private def getTestDataFraction: Double = {
    val testDataFraction: Double = doubleOptionValue(TEST_DATA_FRACTION)
    assert(testDataFraction >= 0 && testDataFraction <= 1, s"Bad testDataFraction: $testDataFraction")
    testDataFraction
  }

  override def createInputData(seed: Long) = {
    val trainingDataPath: String = optionValue[String]("training-data")
    val (rdds, categoricalFeaturesInfo_, numClasses) = if (trainingDataPath != "") {
      println(s"LOADING FILE: $trainingDataPath")
      val numPartitions: Int = intOptionValue(NUM_PARTITIONS)
      val testDataPath: String = optionValue[String]("test-data")
      val testDataFraction: Double = getTestDataFraction
      DataLoader.loadLibSVMFiles(sc, numPartitions, trainingDataPath, testDataPath,
        testDataFraction, seed)
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
  private def createSyntheticInputData(
      seed: Long): (Array[RDD[LabeledPoint]], Map[Int, Int], Int) = {
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
    (splits, categoricalFeaturesInfo_, labelType)
  }

  // TODO: generate DataFrame outside of `runTest` so it is not included in timing results
  override def runTest(rdd: RDD[LabeledPoint]): TreeBasedModel = {
    val treeDepth: Int = intOptionValue(TREE_DEPTH)
    val maxBins: Int = intOptionValue(MAX_BINS)
    val numTrees: Int = intOptionValue(NUM_TREES)
    val featureSubsetStrategy: String = stringOptionValue(FEATURE_SUBSET_STRATEGY)
    val ensembleType: String = stringOptionValue(ENSEMBLE_TYPE)
    if (!supportedTreeTypes.contains(ensembleType)) {
      throw new IllegalArgumentException(
        s"DecisionTreeTest given unknown ensembleType param: $ensembleType." +
        " Supported values: " + supportedTreeTypes.mkString(" "))
    }
    if (labelType == 0) {
      // Regression
      ensembleType match {
        case "RandomForest" =>
          MLlibRFModel(RandomForest.trainRegressor(rdd, categoricalFeaturesInfo, numTrees,
            featureSubsetStrategy, "variance", treeDepth, maxBins, this.getRandomSeed))
        case "ml.RandomForest" =>
          val labelType: Int = intOptionValue(LABEL_TYPE)
          val dataset = DataGenerator.setMetadata(rdd, categoricalFeaturesInfo, labelType)
          val model = new RandomForestRegressor()
            .setImpurity("variance")
            .setMaxDepth(treeDepth)
            .setMaxBins(maxBins)
            .setNumTrees(numTrees)
            .setFeatureSubsetStrategy(featureSubsetStrategy)
            .setSeed(this.getRandomSeed)
            .fit(dataset)
          MLRFRegressionModel(model)
        case "GradientBoostedTrees" =>
          val treeStrategy = new Strategy(Algo.Regression, Variance, treeDepth,
            labelType, maxBins, QuantileStrategy.Sort, categoricalFeaturesInfo)
          val boostingStrategy = BoostingStrategy(treeStrategy, SquaredError, numTrees,
            learningRate = 0.1)
          MLlibGBTModel(GradientBoostedTrees.train(rdd, boostingStrategy))
        case "ml.GradientBoostedTrees" =>
          val labelType: Int = intOptionValue(LABEL_TYPE)
          val dataset = DataGenerator.setMetadata(rdd, categoricalFeaturesInfo, labelType)
          val model = new GBTRegressor()
            .setLossType("squared")
            .setMaxBins(maxBins)
            .setMaxDepth(treeDepth)
            .setMaxIter(numTrees)
            .setStepSize(0.1)
            .setSeed(this.getRandomSeed)
            .fit(dataset)
          MLGBTRegressionModel(model)
      }
    } else if (labelType >= 2) {
      // Classification
      ensembleType match {
        case "RandomForest" =>
          MLlibRFModel(RandomForest.trainClassifier(rdd, labelType, categoricalFeaturesInfo, numTrees,
            featureSubsetStrategy, "gini", treeDepth, maxBins, this.getRandomSeed))
        case "ml.RandomForest" =>
          val labelType: Int = intOptionValue(LABEL_TYPE)
          val dataset = DataGenerator.setMetadata(rdd, categoricalFeaturesInfo, labelType)
          val model = new RandomForestClassifier()
            .setImpurity("gini")
            .setMaxDepth(treeDepth)
            .setMaxBins(maxBins)
            .setNumTrees(numTrees)
            .setFeatureSubsetStrategy(featureSubsetStrategy)
            .setSeed(this.getRandomSeed)
            .fit(dataset)
          MLRFClassificationModel(model)
        case "GradientBoostedTrees" =>
          val treeStrategy = new Strategy(Algo.Classification, Variance, treeDepth,
            labelType, maxBins, QuantileStrategy.Sort, categoricalFeaturesInfo)
          val boostingStrategy = BoostingStrategy(treeStrategy, LogLoss, numTrees,
            learningRate = 0.1)
          MLlibGBTModel(GradientBoostedTrees.train(rdd, boostingStrategy))
        case "ml.GradientBoostedTrees" =>
          val labelType: Int = intOptionValue(LABEL_TYPE)
          val dataset = DataGenerator.setMetadata(rdd, categoricalFeaturesInfo, labelType)
          val model = new GBTClassifier()
            .setLossType("logistic")
            .setMaxBins(maxBins)
            .setMaxDepth(treeDepth)
            .setMaxIter(numTrees)
            .setStepSize(0.1)
            .setSeed(this.getRandomSeed)
            .fit(dataset)
          MLGBTClassificationModel(model)
      }
    } else {
      throw new IllegalArgumentException(s"Bad label-type parameter " +
        s"given to DecisionTreeTest: $labelType")
    }
  }
}
