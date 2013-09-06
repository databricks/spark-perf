package spark.perf

import spark.SparkContext

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println("TestRunner requires 2 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val testName = args(0)
    val master = args(1)
    val otherArgs = args.slice(2, args.length)
    val sc = new SparkContext(master, "Test Runner: " + testName, null,
      Seq("./target/perf-tests-assembly.jar"))

    val test =
      testName match {
        // Spark tests.
        case "aggregate-by-key" => new AggregateByKey(sc)
        case "sort-by-key" => new SortByKey(sc)
        case "count" => new Count(sc)
        case "count-with-filter" => new CountWithFilter(sc)
        // Shark tests.
        // TODO(harvey): Add some tests here...
    }
    test.initialize(otherArgs)
    test.createInputData()
    val results = test.run()
    println("results: " + results.map(r => "%.3f".format(r)).mkString(","))
  }
}
