package spark.perf

trait PerfTest {
  /** Unique identifier for this test */
  def getName: String

  /** Version of the test */
  def getVersion: Int

  /** Initialize internal state based on arguments */
  def initialize(args: Array[String])

  /** Get parameters used in during the trials. */
  def getParams: Seq[(String, String)]

  /** Create stored or otherwise materialized input data */
  def createInputData()

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): Seq[Double]
}
