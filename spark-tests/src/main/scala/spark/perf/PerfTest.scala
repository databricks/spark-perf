package spark.perf

import org.json4s._

trait PerfTest {
  /** Initialize internal state based on arguments */
  def initialize(args: Array[String])

  /** Create stored or otherwise materialized input data */
  def createInputData()

  /**
   * Runs the test and returns a JSON object that captures performance metrics, such as time taken,
   * and values of any parameters.
   *
   * The rendered JSON will look like this (except it will be minified):
   *
   *    {
   *       "options": {
   *         "num-partitions": "10",
   *         "unique-values": "10",
   *         ...
   *       },
   *       "results": [
   *         {
   *           "time": 0.211
   *         },
   *         {
   *           "time": 0.112
   *         }
   *         ...
   *       ]
   *     }
   *
   * @return (options, list of per-run metrics (e.g. ("time" -> time))
   */
  def run(): (JValue, Seq[JValue])
}
