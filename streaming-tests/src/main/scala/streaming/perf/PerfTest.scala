package streaming.perf

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import joptsimple.{OptionSet, OptionParser}

abstract class PerfTest extends Logging {

  /** Int-type command line options expected for this test */
  def intOptions: Seq[(String, String, Boolean)] = Seq(PerfTest.BATCH_DURATION)

  /** String-type command line options expected for this test */
  def stringOptions: Seq[(String, String, Boolean)] = Seq()

  /** Boolean-type ("true" / "false") command line options expected for this test */
  def booleanOptions: Seq[(String, String, Boolean)] = Seq()

  /** Initialize internal state based on arguments */
  def initialize(testName_ : String, otherArgs: Array[String]) {
    testName = testName_
    optionSet = parser.parse(otherArgs:_*)
    batchDuration = optionSet.valueOf(PerfTest.BATCH_DURATION._1).asInstanceOf[Int]
    ssc = createContext()
  }

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): String

  val parser = new OptionParser()
  var optionSet: OptionSet = _
  var testName: String = _
  var batchDuration: Int = _
  var ssc: StreamingContext = _

  // add all the options to parser
  intOptions.map{case (opt, desc, reqd) =>
    val temp = parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Int])
    if (reqd)  temp.required()
  }
  stringOptions.map{case (opt, desc, reqd) =>
    val temp = parser.accepts(opt, desc).withRequiredArg().ofType(classOf[String])
    if (reqd)  temp.required()
  }
  booleanOptions.map{case (opt, desc, reqd) =>
    val temp = parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Boolean])
    if (reqd)  temp.required()
  }

  protected def createContext() = {
    val conf = new SparkConf().setAppName(testName)
    val sparkContext = new SparkContext(new SparkConf())
    new StreamingContext(sparkContext,
      Milliseconds(batchDuration))
  }

  def intOptionValue(option: (String, String, Boolean)) = optionSet.valueOf(option._1).asInstanceOf[Int]

  def stringOptionValue(option: (String, String, Boolean)) = optionSet.valueOf(option._1).asInstanceOf[String]

  def booleanOptionValue(option: (String, String, Boolean)) = optionSet.valueOf(option._1).asInstanceOf[Boolean]
}

object PerfTest {
  val BATCH_DURATION = ("batch-duration", "duration of the batch size in milliseconds", true)
}
