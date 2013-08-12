package spark.perf

trait PerfTest {
  /** Initialize internal state based on arguments */
  def initialize(args: Array[String])

  /** Create stored or otherwise materialized input data */
  def createInputData()

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): Seq[Double]
}
