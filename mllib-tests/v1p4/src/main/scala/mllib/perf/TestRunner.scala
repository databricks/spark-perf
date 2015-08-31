package mllib.perf

import scala.collection.JavaConverters._

import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkContext}

import mllib.perf.clustering.{EMLDATest, GaussianMixtureTest, OnlineLDATest, PICTest}
import mllib.perf.feature.Word2VecTest
import mllib.perf.fpm.FPGrowthTest
import mllib.perf.linalg.BlockMatrixMultTest

object TestRunner {
    def main(args: Array[String]) {
      if (args.size < 1) {
        println(
          "mllib.perf.TestRunner requires 1 or more args, you gave %s, exiting".format(args.size))
        System.exit(1)
      }
      val testName = args(0)
      val perfTestArgs = args.slice(1, args.length)
      val sc = new SparkContext(new SparkConf().setAppName("TestRunner: " + testName))

      // Unfortunate copy of code because there are Perf Tests in both projects and the compiler doesn't like it
      val test: PerfTest = testName match {
        case "glm-regression" => new GLMRegressionTest(sc)
        case "glm-classification" => new GLMClassificationTest(sc)
        case "naive-bayes" => new NaiveBayesTest(sc)
        // recommendation
        case "als" => new ALSTest(sc)
        // clustering
        case "kmeans" => new KMeansTest(sc)
        case "gmm" => new GaussianMixtureTest(sc)
        case "emlda" => new EMLDATest(sc)
        case "onlinelda" => new OnlineLDATest(sc)
        case "pic" => new PICTest(sc)
        // trees
        case "decision-tree" => new DecisionTreeTest(sc)
        // linalg
        case "svd" => new SVDTest(sc)
        case "pca" => new PCATest(sc)
        // stats
        case "summary-statistics" => new ColumnSummaryStatisticsTest(sc)
        case "pearson" => new PearsonCorrelationTest(sc)
        case "spearman" => new SpearmanCorrelationTest(sc)
        case "chi-sq-feature" => new ChiSquaredFeatureTest(sc)
        case "chi-sq-gof" => new ChiSquaredGoFTest(sc)
        case "chi-sq-mat" => new ChiSquaredMatTest(sc)
        case "fp-growth" => new FPGrowthTest(sc)
        case "block-matrix-mult" => new BlockMatrixMultTest(sc)
        case "word2vec" => new Word2VecTest(sc)
      }
      test.initialize(testName, perfTestArgs)
      // Generate a new dataset for each test
      val rand = new java.util.Random(test.getRandomSeed)

      val numTrials = test.getNumTrials
      val interTrialWait = test.getWait

      var testOptions: JValue = test.getOptions
      val results: Seq[JValue] = (1 to numTrials).map { i =>
        test.createInputData(rand.nextLong())
        val res: JValue = test.run()
        System.gc()
        Thread.sleep(interTrialWait)
        res
      }
      // Report the test results as a JSON object describing the test options, Spark
      // configuration, Java system properties, as well as the per-test results.
      // This extra information helps to ensure reproducibility and makes automatic analysis easier.
      val json: JValue =
        ("testName" -> testName) ~
        ("options" -> testOptions) ~
        ("sparkConf" -> sc.getConf.getAll.toMap) ~
        ("sparkVersion" -> sc.version) ~
        ("systemProperties" -> System.getProperties.asScala.toMap) ~
        ("results" -> results)
      println("results: " + compact(render(json)))

      sc.stop()
  }
}
