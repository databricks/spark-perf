package mllib.perf.onepointone

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

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
      val test: PerfTest =
        testName match {
          case "lr-lbfgs" => new LogisticRegressionWithLBFGSTest(sc)
          case "decision-tree" => new DecisionTreeTest(sc)
          case "pearson" => new PearsonCorrelationTest(sc)
          case "spearman" => new SpearmanCorrelationTest(sc)
          case "chi-sq-feature" => new ChiSquaredFeatureTest(sc)
          case "chi-sq-gof" => new ChiSquaredGoFTest(sc)
          case "chi-sq-mat" => new ChiSquaredMatTest(sc)
        }
      test.initialize(testName, perfTestArgs)
      // Generate a new dataset for each test
      val rand = new java.util.Random(test.getRandomSeed)

      val numTrials = test.getNumTrials
      val interTrialWait = test.getWait

      val results: Seq[(Double, Double, Double)] = (1 to numTrials).map { i =>
        test.createInputData(rand.nextLong())
        val data = test.run()

        System.gc()
        Thread.sleep(interTrialWait)

        data
      }

      println("results: " + results.map(r => "%.3f;%.3f;%.3f".format(r._1, r._2, r._3)).mkString(","))

      sc.stop()
  }
}
