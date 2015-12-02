package streaming.perf

import org.apache.spark.SparkContext

class BasicTest(sc: SparkContext) extends PerfTest(sc) {

  override def run(): String = {
    doRunPerf()
    "PASSED"
  }

  override def doRunPerf(): Seq[(String, Double)] = {
    println("Running BasicTest.run()")
    println(sc.makeRDD(1 to 100, 10).collect().mkString(", "))
    println("Successfully run spark job")
    Nil
  }
}
