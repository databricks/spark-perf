package streaming.perf

import org.apache.spark.streaming.StreamingContext

object TestRunner {
  def main(args: Array[String]) {
    if (args.size < 2) {
      println("spark.perf.TestRunner requires 2 or more args, you gave %s, exiting".format(args.size))
      System.exit(1)
    }
    val testName = args(0)
    val master = args(1)
    val perfTestArgs = args.slice(2, args.length)
    println("Test name = " + testName)
    println("Master = " + master)
    println("Test args = " + perfTestArgs.mkString(" "))
    val test: PerfTest = testName match {
      case "basic" => new BasicTest()
      case _ => throw new IllegalArgumentException("Unexpected test name - " + testName)
    }
    test.initialize(testName, master, perfTestArgs)
    val results = test.run()
    println("results: " + results.mkString(","))
  }
}

