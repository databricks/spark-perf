package mllib.perf.util

import org.apache.spark.ml.attribute.{AttributeGroup, NumericAttribute, NominalAttribute}
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.collection.mutable

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.random._
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.model.{Split, DecisionTreeModel, Node, Predict}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.SparkContext

object DataGenerator {

  def generateLabeledPoints(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      intercept: Double,
      labelNoise: Double,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis(),
      problem: String = ""): RDD[LabeledPoint] = {

    RandomRDDs.randomRDD(sc, new LinearDataGenerator(numCols,intercept, seed, labelNoise, problem),
      numRows, numPartitions, seed)

  }

  def generateDistributedSquareMatrix(
      sc: SparkContext,
      m: Long,
      n: Int,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis()): RowMatrix = {

    val data: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, m, n, numPartitions, seed)

    new RowMatrix(data,m,n)
  }

  def generateClassificationLabeledPoints(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      threshold: Double,
      featureNoise: Double,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis(),
      chiSq: Boolean = false): RDD[LabeledPoint] = {

    RandomRDDs.randomRDD(sc, new ClassLabelGenerator(numCols,threshold, featureNoise, chiSq),
      numRows, numPartitions, seed)
  }

  def generateBinaryLabeledPoints(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      threshold: Double,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis()): RDD[LabeledPoint] = {

    RandomRDDs.randomRDD(sc, new BinaryLabeledDataGenerator(numCols,threshold),
      numRows, numPartitions, seed)
  }

  /**
   * @param labelType  0 = regression with labels in [0,1].  Values >= 2 indicate classification.
   * @param fracCategorical  Fraction of columns/features to be categorical.
   * @param fracBinary   Fraction of categorical features to be binary.  Others are high-arity (20).
   * @param treeDepth  Depth of "true" tree used to label points.
   * @return (data, categoricalFeaturesInfo)
   *         data is an RDD of data points.
   *         categoricalFeaturesInfo is a map storing the arity of categorical features.
   *         E.g., an entry (n -> k) indicates that feature n is categorical
   *         with k categories indexed from 0: {0, 1, ..., k-1}.
   */
  def generateDecisionTreeLabeledPoints(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      labelType: Int,
      fracCategorical: Double,
      fracBinary: Double,
      treeDepth: Int,
      seed: Long = System.currentTimeMillis()): (RDD[LabeledPoint], Map[Int, Int]) = {

    val highArity = 20

    require(fracCategorical >= 0 && fracCategorical <= 1,
      s"fracCategorical must be in [0,1], but it is $fracCategorical")
    require(fracBinary >= 0 && fracBinary <= 1,
      s"fracBinary must be in [0,1], but it is $fracBinary")

    val isRegression = labelType == 0
    if (!isRegression) {
      require(labelType >= 2, s"labelType must be >= 2 for classification. 0 indicates regression.")
    }
    val numCategorical = (numCols * fracCategorical).toInt
    val numContinuous = numCols - numCategorical
    val numBinary = (numCategorical * fracBinary).toInt
    val numHighArity = numCategorical - numBinary
    val categoricalArities = Array.concat(Array.fill(numBinary)(2),
      Array.fill(numHighArity)(highArity))

    val featuresGenerator = new FeaturesGenerator(categoricalArities, numContinuous)
    val featureMatrix = RandomRDDs.randomRDD(sc, featuresGenerator,
      numRows, numPartitions, seed)

    // Create random DecisionTree.
    val featureArity = Array.concat(categoricalArities, Array.fill(numContinuous)(0))
    val trueModel = randomBalancedDecisionTree(treeDepth, labelType, featureArity, seed)
    println(trueModel)

    // Label points using tree.
    val labelVector = featureMatrix.map(trueModel.predict)

    val data = labelVector.zip(featureMatrix).map(pair => new LabeledPoint(pair._1, pair._2))
    val categoricalFeaturesInfo = featuresGenerator.getCategoricalFeaturesInfo
    (data, categoricalFeaturesInfo)
  }

  /**
   * From spark.ml.impl.TreeTests
   *
   * Convert the given data to a DataFrame, and set the features and label metadata.
   * @param data  Dataset.  Categorical features and labels must already have 0-based indices.
   *              This must be non-empty.
   * @param categoricalFeatures  Map: categorical feature index -> number of distinct values
   * @param numClasses  Number of classes label can take.  If 0, mark as continuous.
   * @return DataFrame with metadata
   */
  def setMetadata(
      data: RDD[LabeledPoint],
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): DataFrame = {
    val sqlContext = SQLContext.getOrCreate(data.sparkContext)
    import sqlContext.implicits._
    val df = data.toDF()
    val numFeatures = data.first().features.size
    val featuresAttributes = Range(0, numFeatures).map { feature =>
      if (categoricalFeatures.contains(feature)) {
        NominalAttribute.defaultAttr.withIndex(feature).withNumValues(categoricalFeatures(feature))
      } else {
        NumericAttribute.defaultAttr.withIndex(feature)
      }
    }.toArray
    val featuresMetadata = new AttributeGroup("features", featuresAttributes).toMetadata()
    val labelAttribute = if (numClasses == 0) {
      NumericAttribute.defaultAttr.withName("label")
    } else {
      NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses)
    }
    val labelMetadata = labelAttribute.toMetadata()
    df.select(df("features").as("features", featuresMetadata),
      df("label").as("label", labelMetadata))
  }


  def randomBalancedDecisionTree(
      depth: Int,
      labelType: Int,
      featureArity: Array[Int],
      seed: Long = System.currentTimeMillis()): DecisionTreeModel = {

    require(depth >= 0, s"randomBalancedDecisionTree given depth < 0.")
    require(depth <= featureArity.size,
      s"randomBalancedDecisionTree requires depth <= featureArity.size," +
      s" but depth = $depth and featureArity.size = ${featureArity.size}")
    val isRegression = labelType == 0
    if (!isRegression) {
      require(labelType >= 2, s"labelType must be >= 2 for classification. 0 indicates regression.")
    }

    val rng = new scala.util.Random()
    rng.setSeed(seed)

    val labelGenerator = if (isRegression) {
      new RealLabelPairGenerator()
    } else {
      new ClassLabelPairGenerator(labelType)
    }

    val topNode = randomBalancedDecisionTreeHelper(0, depth, featureArity, labelGenerator,
      Set.empty, rng)
    if (isRegression) {
      new DecisionTreeModel(topNode, Algo.Regression)
    } else {
      new DecisionTreeModel(topNode, Algo.Classification)
    }
  }

  /**
   * Create an internal node.  Either create the leaf nodes beneath it, or recurse as needed.
   * @param nodeIndex  Index of node.
   * @param subtreeDepth  Depth of subtree to build.  Depth 0 means this is a leaf node.
   * @param featureArity  Indicates feature type.  Value 0 indicates continuous feature.
   *                      Other values >= 2 indicate a categorical feature,
   *                      where the value is the number of categories.
   * @param usedFeatures  Features appearing in the path from the tree root to the node
   *                      being constructed.
   * @param labelGenerator  Generates pairs of distinct labels.
   * @return
   */
  def randomBalancedDecisionTreeHelper(
      nodeIndex: Int,
      subtreeDepth: Int,
      featureArity: Array[Int],
      labelGenerator: RandomDataGenerator[Pair[Double, Double]],
      usedFeatures: Set[Int],
      rng: scala.util.Random): Node = {

    if (subtreeDepth == 0) {
      // This case only happens for a depth 0 tree.
      return new Node(id = nodeIndex, predict = new Predict(0), impurity = 0, isLeaf = true,
        split = None, leftNode = None, rightNode = None, stats = None)
    }

    val numFeatures = featureArity.size
    if (usedFeatures.size >= numFeatures) {
      // Should not happen.
      throw new RuntimeException(s"randomBalancedDecisionTreeSplitNode ran out of " +
        s"features for splits.")
    }

    // Make node internal.
    var feature: Int = rng.nextInt(numFeatures)
    while (usedFeatures.contains(feature)) {
      feature = rng.nextInt(numFeatures)
    }
    val split: Split = if (featureArity(feature) == 0) {
      // continuous feature
      new Split(feature = feature, threshold = rng.nextDouble(),
        featureType = FeatureType.Continuous, categories = List())
    } else {
      // categorical feature
      // Put nCatsSplit categories on left, and the rest on the right.
      // nCatsSplit is in {1,...,arity-1}.
      val nCatsSplit = rng.nextInt(featureArity(feature) - 1) + 1
      val splitCategories = rng.shuffle(Range(0,featureArity(feature)).toList).take(nCatsSplit)
      new Split(feature = feature, threshold = 0,
        featureType = FeatureType.Categorical, categories =
          splitCategories.asInstanceOf[List[Double]])
    }

    val leftChildIndex = nodeIndex * 2 + 1
    val rightChildIndex = nodeIndex * 2 + 2
    if (subtreeDepth == 1) {
      // Add leaf nodes.
      val predictions = labelGenerator.nextValue()
      new Node(id = nodeIndex, predict = new Predict(0), impurity = 0, isLeaf = false, split = Some(split),
        leftNode = Some(new Node(id = leftChildIndex, predict = new Predict(predictions._1), impurity = 0, isLeaf = true,
          split = None, leftNode = None, rightNode = None, stats = None)),
        rightNode = Some(new Node(id = rightChildIndex, predict = new Predict(predictions._2), impurity = 0, isLeaf = true,
          split = None, leftNode = None, rightNode = None, stats = None)), stats = None)
    } else {
      new Node(id = nodeIndex, predict = new Predict(0), impurity = 0, isLeaf = false, split = Some(split),
        leftNode = Some(randomBalancedDecisionTreeHelper(leftChildIndex, subtreeDepth - 1,
          featureArity, labelGenerator, usedFeatures + feature, rng)),
        rightNode = Some(randomBalancedDecisionTreeHelper(rightChildIndex, subtreeDepth - 1,
          featureArity, labelGenerator, usedFeatures + feature, rng)), stats = None)
    }
  }

  def generateKMeansVectors(
      sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numCenters: Int,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis()): RDD[Vector] = {

    RandomRDDs.randomRDD(sc, new KMeansDataGenerator(numCenters, numCols, seed),
      numRows, numPartitions, seed)
  }


  // Problems with having a userID or productID in the test set but not training set
  // leads to a lot of work...
  def generateRatings(
      sc: SparkContext,
      numUsers: Int,
      numProducts: Int,
      numRatings: Long,
      implicitPrefs: Boolean,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis()): (RDD[Rating],RDD[Rating]) = {

    val train = RandomRDDs.randomRDD(sc,
      new RatingGenerator(numUsers, numProducts,implicitPrefs),
      numRatings, numPartitions, seed).cache()

    val test = RandomRDDs.randomRDD(sc,
      new RatingGenerator(numUsers, numProducts,implicitPrefs),
      math.ceil(numRatings * 0.25).toLong, numPartitions, seed + 24)

    // Now get rid of duplicate ratings and remove non-existant userID's
    // and prodID's from the test set
    val commons: PairRDDFunctions[(Int,Int),Rating] =
      new PairRDDFunctions(train.keyBy(rating => (rating.user, rating.product)).cache())

    val exact = commons.join(test.keyBy(rating => (rating.user, rating.product)))

    val trainPruned = commons.subtractByKey(exact).map(_._2).cache()

    // Now get rid of users that don't exist in the train set
    val trainUsers: RDD[(Int,Rating)] = trainPruned.keyBy(rating => rating.user)
    val testUsers: PairRDDFunctions[Int,Rating] =
      new PairRDDFunctions(test.keyBy(rating => rating.user))
    val testWithAdditionalUsers = testUsers.subtractByKey(trainUsers)

    val userPrunedTestProds: RDD[(Int,Rating)] =
      testUsers.subtractByKey(testWithAdditionalUsers).map(_._2).keyBy(rating => rating.product)

    val trainProds: RDD[(Int,Rating)] = trainPruned.keyBy(rating => rating.product)

    val testWithAdditionalProds =
      new PairRDDFunctions[Int, Rating](userPrunedTestProds).subtractByKey(trainProds)
    val finalTest =
      new PairRDDFunctions[Int, Rating](userPrunedTestProds).subtractByKey(testWithAdditionalProds)
        .map(_._2)

    (trainPruned, finalTest)
  }

}

class RatingGenerator(
    private val numUsers: Int,
    private val numProducts: Int,
    private val implicitPrefs: Boolean) extends RandomDataGenerator[Rating] {

  private val rng = new java.util.Random()

  private val observed = new mutable.HashMap[(Int, Int), Boolean]()

  override def nextValue(): Rating = {
    var tuple = (rng.nextInt(numUsers),rng.nextInt(numProducts))
    while (observed.getOrElse(tuple,false)){
      tuple = (rng.nextInt(numUsers),rng.nextInt(numProducts))
    }
    observed += (tuple -> true)

    val rating = if (implicitPrefs) rng.nextInt(2)*1.0 else rng.nextDouble()*5

    new Rating(tuple._1, tuple._2, rating)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): RatingGenerator = new RatingGenerator(numUsers, numProducts, implicitPrefs)
}

// For general classification
class ClassLabelGenerator(
    private val numFeatures: Int,
    private val threshold: Double,
    private val featureNoise: Double,
    private val chiSq: Boolean) extends RandomDataGenerator[LabeledPoint] {

  private val rng = new java.util.Random()

  override def nextValue(): LabeledPoint = {
    val y = if (rng.nextDouble() < threshold) 0.0 else 1.0
    val x = Array.fill[Double](numFeatures) {
      if (!chiSq) rng.nextGaussian() + (y * featureNoise) else rng.nextInt(6) * 1.0
    }

    LabeledPoint(y, Vectors.dense(x))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): ClassLabelGenerator =
    new ClassLabelGenerator(numFeatures, threshold, featureNoise, chiSq)
}

class BinaryLabeledDataGenerator(
  private val numFeatures: Int,
  private val threshold: Double) extends RandomDataGenerator[LabeledPoint] {

  private val rng = new java.util.Random()

  override def nextValue(): LabeledPoint = {
    val y = if (rng.nextDouble() < threshold) 0.0 else 1.0
    val x = Array.fill[Double](numFeatures) {
      if (rng.nextDouble() < threshold) 0.0 else 1.0
    }
    LabeledPoint(y, Vectors.dense(x))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): BinaryLabeledDataGenerator =
    new BinaryLabeledDataGenerator(numFeatures, threshold)

}

class LinearDataGenerator(
    val numFeatures: Int,
    val intercept: Double,
    val seed: Long,
    val labelNoise: Double,
    val problem: String = "",
    val sparsity: Double = 1.0) extends RandomDataGenerator[LabeledPoint] {

  private val rng = new java.util.Random(seed)

  private val weights = Array.fill(numFeatures)(rng.nextDouble())
  private val nnz: Int = math.ceil(numFeatures*sparsity).toInt

  override def nextValue(): LabeledPoint = {
    val x = Array.fill[Double](nnz)(2*rng.nextDouble()-1)

    val y = weights.zip(x).map(p => p._1 * p._2).sum + intercept + labelNoise*rng.nextGaussian()
    val yD =
      if (problem == "SVM"){
        if (y < 0.0) 0.0 else 1.0
      } else{
        y
      }

    LabeledPoint(yD, Vectors.dense(x))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): LinearDataGenerator =
    new LinearDataGenerator(numFeatures, intercept, seed, labelNoise, problem, sparsity)
}


/**
 * Generator for a pair of distinct class labels from the set {0,...,numClasses-1}.
 * @param numClasses  Number of classes.
 */
class ClassLabelPairGenerator(val numClasses: Int)
  extends RandomDataGenerator[Pair[Double, Double]] {

  require(numClasses >= 2,
    s"ClassLabelPairGenerator given label numClasses = $numClasses, but numClasses should be >= 2.")

  private val rng = new java.util.Random()

  override def nextValue(): Pair[Double, Double] = {
    val left = rng.nextInt(numClasses)
    var right = rng.nextInt(numClasses)
    while (right == left) {
      right = rng.nextInt(numClasses)
    }
    new Pair[Double, Double](left, right)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): ClassLabelPairGenerator = new ClassLabelPairGenerator(numClasses)
}


/**
 * Generator for a pair of real-valued labels.
 */
class RealLabelPairGenerator() extends RandomDataGenerator[Pair[Double, Double]] {

  private val rng = new java.util.Random()

  override def nextValue(): Pair[Double, Double] =
    new Pair[Double, Double](rng.nextDouble(), rng.nextDouble())

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): RealLabelPairGenerator = new RealLabelPairGenerator()
}


/**
 * Generator for a feature vector which can include a mix of categorical and continuous features.
 * @param categoricalArities  Specifies the number of categories for each categorical feature.
 * @param numContinuous  Number of continuous features.  Feature values are in range [0,1].
 */
class FeaturesGenerator(val categoricalArities: Array[Int], val numContinuous: Int)
  extends RandomDataGenerator[Vector] {

  categoricalArities.foreach { arity =>
    require(arity >= 2, s"FeaturesGenerator given categorical arity = $arity, " +
      s"but arity should be >= 2.")
  }

  val numFeatures = categoricalArities.size + numContinuous

  private val rng = new java.util.Random()

  /**
   * Generates vector with categorical features first, and continuous features in [0,1] second.
   */
  override def nextValue(): Vector = {
    // Feature ordering matches getCategoricalFeaturesInfo.
    val arr = new Array[Double](numFeatures)
    var j = 0
    while (j < categoricalArities.size) {
      arr(j) = rng.nextInt(categoricalArities(j))
      j += 1
    }
    while (j < numFeatures) {
      arr(j) = rng.nextDouble()
      j += 1
    }
    Vectors.dense(arr)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): FeaturesGenerator = new FeaturesGenerator(categoricalArities, numContinuous)

  /**
   * @return categoricalFeaturesInfo Map storing arity of categorical features.
   *                                 E.g., an entry (n -> k) indicates that feature n is categorical
   *                                 with k categories indexed from 0: {0, 1, ..., k-1}.
   */
  def getCategoricalFeaturesInfo: Map[Int, Int] = {
    // Categorical features are indexed from 0 because of the implementation of nextValue().
    categoricalArities.zipWithIndex.map(_.swap).toMap
  }

}


class KMeansDataGenerator(
    val numCenters: Int,
    val numFeatures: Int,
    val seed: Long) extends RandomDataGenerator[Vector] {

  private val rng = new java.util.Random(seed)
  private val rng2 = new java.util.Random(seed + 24)
  private val scale_factors = Array.fill(numCenters)(rng.nextInt(20) - 10)

  // Have a random number of points around a cluster
  private val concentrations: Seq[Double] = {
    val rand = Array.fill(numCenters)(rng.nextDouble())
    val randSum = rand.sum
    val scaled = rand.map(x => x / randSum)

    (1 to numCenters).map{i =>
      scaled.slice(0, i).sum
    }
  }

  private val centers = (0 until numCenters).map{i =>
    Array.fill(numFeatures)((2 * rng.nextDouble() - 1)*scale_factors(i))
  }

  override def nextValue(): Vector = {
    val pick_center_rand = rng2.nextDouble()

    val centerToAddTo = centers(concentrations.indexWhere(p => pick_center_rand <= p))

    Vectors.dense(Array.tabulate(numFeatures)(i => centerToAddTo(i) + rng2.nextGaussian()))
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): KMeansDataGenerator = new KMeansDataGenerator(numCenters, numFeatures, seed)
}
