package spark.perf

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

    val test: PerfTest =
      testName match {
        case "aggregate-by-key" => new AggregateByKey(sc)
        case "aggregate-by-key-int" => new AggregateByKeyInt(sc)
        case "sort-by-key" => new SortByKey(sc)
        case "sort-by-key-int" => new SortByKeyInt(sc)
        case "count" => new Count(sc)
        case "count-with-filter" => new CountWithFilter(sc)
        case "scheduling-throughput" => new SchedulerThroughputTest(sc)
    }
    test.initialize(perfTestArgs)
    test.createInputData()
    val results: Seq[Double] = test.run()
    println("results: " + results.map(r => "%.3f".format(r)).mkString(","))
  }
}
