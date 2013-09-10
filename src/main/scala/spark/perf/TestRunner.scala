package spark.perf

import spark.SparkContext

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println(
        "spark.perf.TestRunner requires 2 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val testName = args(0)
    val master = args(1)
    val perfTestArgs = args.slice(2, args.length)
    val sc = new SparkContext(master, "TestRunner: " + testName, System.getenv("SPARK_HOME"),
      Seq(System.getProperty("user.dir") + "/target/perf-tests-assembly.jar"))

    val test: PerfTest =
      testName match {
        case "aggregate-by-key" => new AggregateByKey(sc)
        case "sort-by-key" => new SortByKey(sc)
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
