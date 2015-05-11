package mllib.perf

import scala.collection.JavaConverters._

import joptsimple.{OptionSet, OptionParser}

import org.json4s._

import org.apache.spark.Logging

abstract class PerfTest extends Logging {

  val NUM_TRIALS =          ("num-trials",    "number of trials to run")
  val INTER_TRIAL_WAIT =    ("inter-trial-wait",   "seconds to sleep between trials")
  val NUM_PARTITIONS =      ("num-partitions", "number of input partitions")
  val RANDOM_SEED =         ("random-seed", "seed for random number generator")

  /** Initialize internal state based on arguments */
  def initialize(testName_ : String, otherArgs: Array[String]) {
    testName = testName_
    optionSet = parser.parse(otherArgs:_*)
  }

  def getRandomSeed: Int = {
    intOptionValue(RANDOM_SEED)
  }

  def getNumTrials: Int = {
    intOptionValue(NUM_TRIALS)
  }

  def getWait: Int = {
    intOptionValue(INTER_TRIAL_WAIT) * 1000
  }

  def createInputData(seed: Long)

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
   *           "trainingTime": 0.211,
   *           "trainingMetric": 98.1,
   *           ...
   *         },
   *         ...
   *       ]
   *     }
   *
   * @return metrics from run (e.g. ("time" -> time)
   *  */
  def run(): JValue

  val parser = new OptionParser()
  var optionSet: OptionSet = _
  var testName: String = _

  var intOptions: Seq[(String, String)] = Seq(NUM_TRIALS, INTER_TRIAL_WAIT, NUM_PARTITIONS,
    RANDOM_SEED)

  var doubleOptions: Seq[(String, String)] = Seq()
  var longOptions: Seq[(String, String)] = Seq()

  var stringOptions: Seq[(String, String)] = Seq()
  var booleanOptions: Seq[(String, String)] = Seq()

  def addOptionsToParser() {
    // add all the options to parser
    stringOptions.map{case (opt, desc) =>
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[String]).required()
    }
    booleanOptions.map{case (opt, desc) =>
      parser.accepts(opt, desc)
    }
    intOptions.map{case (opt, desc) =>
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Int]).required()
    }
    doubleOptions.map{case (opt, desc) =>
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Double]).required()
    }
    longOptions.map{case (opt, desc) =>
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Long]).required()
    }
  }

  def addOptionalOptionToParser[T](opt: String, desc: String, default: T, clazz: Class[T]): Unit = {
    parser.accepts(opt, desc).withOptionalArg().ofType(clazz).defaultsTo(default)
  }

  def intOptionValue(option: (String, String)) =
    optionSet.valueOf(option._1).asInstanceOf[Int]

  def stringOptionValue(option: (String, String)) =
    optionSet.valueOf(option._1).asInstanceOf[String]

  def booleanOptionValue(option: (String, String)) =
    optionSet.has(option._1)

  def doubleOptionValue(option: (String, String)) =
    optionSet.valueOf(option._1).asInstanceOf[Double]

  def longOptionValue(option: (String, String)) =
    optionSet.valueOf(option._1).asInstanceOf[Long]

  def optionValue[T](option: String) =
    optionSet.valueOf(option).asInstanceOf[T]

  def getOptions: Map[String, String] = {
    optionSet.asMap().asScala.flatMap { case (spec, values) =>
      if (spec.options().size() == 1 && values.size() == 1) {
        Some((spec.options().iterator().next(), values.iterator().next().toString))
      } else {
        None
      }
    }.toMap
  }
}
