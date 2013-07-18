package spark.perf

import spark.SparkContext

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 4) {
      println("TestRunner requires 4 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val master = args(1)
    val testName = args(2)
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
