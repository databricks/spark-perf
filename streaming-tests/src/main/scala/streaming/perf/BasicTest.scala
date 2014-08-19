package streaming.perf

class BasicTest extends PerfTest {

  def run(): String =  {
    println("Running BasicTest.run()")
    println(ssc.sparkContext.makeRDD(1 to 100, 10).collect().mkString(", "))
    println("Successfully run spark job")
    "PASSED"
  }
}
