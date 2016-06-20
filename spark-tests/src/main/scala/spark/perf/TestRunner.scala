package spark.perf

import scala.collection.JavaConverters._

import org.json4s.JsonDSL._
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 1) {
      println(
        "spark.perf.TestRunner requires 1 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val testName = args(0)
    val perfTestArgs = args.slice(1, args.length)
    val sc = new SparkContext(new SparkConf().setAppName("TestRunner: " + testName))

    val test: PerfTest = testName match {
      case "aggregate-by-key" => new AggregateByKey(sc)
      case "aggregate-by-key-int" => new AggregateByKeyInt(sc)
      case "aggregate-by-key-naive" => new AggregateByKeyNaive(sc)
      case "sort-by-key" => new SortByKey(sc)
      case "sort-by-key-int" => new SortByKeyInt(sc)
      case "count" => new Count(sc)
      case "count-with-filter" => new CountWithFilter(sc)
      case "scheduling-throughput" => new SchedulerThroughputTest(sc)
    }
    test.initialize(perfTestArgs)
    test.createInputData()
    val (testOptions: JValue, results: Seq[JValue]) = test.run()

    // Report the test results as a JSON object describing the test options, Spark
    // configuration, Java system properties, as well as the per-test times.
    // This extra information helps to ensure reproducibility and makes automatic analysis easier.
    val json: JValue =
      ("testName" -> testName) ~
      ("options" -> testOptions) ~
      ("sparkConf" -> sc.getConf.getAll.toMap) ~
      ("sparkVersion" -> sc.version) ~
      ("systemProperties" -> System.getProperties.asScala.toMap) ~
      ("results" -> results)
    println("results: " + compact(json))

    // Gracefully stop the SparkContext so that the application web UI can be preserved
    // and viewed using the HistoryServer.
    sc.stop()
    System.exit(0)
  }
}
