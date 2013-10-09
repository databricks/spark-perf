package spark.perf

import org.apache.spark.SparkContext

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
      Seq(System.getProperty("user.dir") + "/spark-tests/target/scala-2.9.3/spark-perf-tests-assembly.jar"))

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
