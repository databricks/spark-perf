package streaming.perf

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import joptsimple.{OptionSet, OptionParser}

abstract class PerfTest {

  val BATCH_DURATION = ("batch-duration", "duration of the batch size in milliseconds")
  val TOTAL_DURATION = ("total-duration", "Total duration of the test in seconds")
  val HDFS_URL = ("hdfs-url", "URL of the HDFS directory that is to be used for this test")

  val parser = new OptionParser()
  val jarFile = System.getProperty("user.dir", "..") + "/streaming-tests/target/streaming-perf-tests-assembly.jar"
  val sparkDir = Option(System.getenv("SPARK_HOME")).getOrElse("../spark/")

  var optionSet: OptionSet = _
  var testName: String = _
  var batchDurationMs: Long = _
  var totalDurationSec: Long = _
  var hdfsUrl: String = _
  var checkpointDirectory: String = _
  var ssc: StreamingContext = _
  var sc: SparkContext = _

  /** Long-type command line options expected for this test */
  def longOptions: Seq[(String, String)] = Seq(BATCH_DURATION, TOTAL_DURATION)

  /** String-type command line options expected for this test */
  def stringOptions: Seq[(String, String)] = Seq(HDFS_URL)

  /** Boolean-type ("true" / "false") command line options expected for this test */
  def booleanOptions: Seq[(String, String)] = Seq()

  /** Initialize internal state based on arguments */
  def initialize(testName_ : String, otherArgs: Array[String]) {
    // add all the options to parser
    longOptions.map{case (opt, desc) =>
      println("Registering long option " + opt)
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Long]).required()
    }
    stringOptions.map{case (opt, desc) =>
      println("Registering string option " + opt)
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[String]).required()
    }
    booleanOptions.map{case (opt, desc) =>
      println("Registering boolean option " + opt)
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Boolean]).required()
    }

    testName = testName_
    optionSet = parser.parse(otherArgs:_*)
    batchDurationMs = longOptionValue(BATCH_DURATION)
    totalDurationSec = longOptionValue(TOTAL_DURATION)
    hdfsUrl = stringOptionValue(HDFS_URL)
    checkpointDirectory = hdfsUrl + "/checkpoint/"
    ssc = createContext()
    ssc.checkpoint(checkpointDirectory)
    sc = ssc.sparkContext
  }

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): String

  protected def createContext() = {
    val conf = new SparkConf().setAppName(testName)
    val sparkContext = new SparkContext(conf)    
    new StreamingContext(sparkContext, Milliseconds(batchDurationMs))
  }

  /** Get value of long-type command line option */
  def longOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[Long]

  /** Get value of string-type command line option */
  def stringOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[String]

  /** Get value of boolean-type ("true" / "false") command line option */
  def booleanOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[Boolean]
}
