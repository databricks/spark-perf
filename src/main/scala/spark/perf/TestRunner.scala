package spark.perf

import spark.SparkContext

object TestRunner {
  def main(args: Array[String]) {
    val testName = args(1)
    val master = args(2)
    val otherArgs = args.slice(3, args.length)
    val sc = new SparkContext(master, "Test Runner: " + testName, null,
      Seq("./target/scala-2.9.3/spark-perf_2.9.3-0.1.jar"))

    val test =
      testName match {
        case "aggregate-by-key" => new AggregateByKey(sc)
        case "sort-by-key" => new SortByKey(sc)
        case "count" => new Count(sc)
        case "count-with-filter" => new CountWithFilter(sc)
    }
    test.initialize(otherArgs)
    test.createInputData()
    val results = test.run()
    println(results.map(r => "%.3f".format(r)).mkString(","))
  }
}
