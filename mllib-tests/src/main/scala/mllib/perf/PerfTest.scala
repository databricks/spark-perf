package mllib.perf

import org.apache.spark.Logging
import joptsimple.{OptionSet, OptionParser}

abstract class PerfTest extends Logging {

  val NUM_TRIALS =          ("num-trials",    "number of trials to run")
  val INTER_TRIAL_WAIT =    ("inter-trial-wait",   "seconds to sleep between trials")
  val NUM_PARTITIONS =      ("num-partitions", "number of input partitions")
  val RANDOM_SEED =         ("random-seed", "seed for random number generator")
  val NUM_ITERATIONS =      ("num-iterations",   "number of iterations for the algorithm")
  val REGULARIZATION =      ("reg-param",   "the regularization parameter against overfitting")

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
    intOptionValue(INTER_TRIAL_WAIT)*1000
  }

  def createInputData(seed: Long)

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): (Double, Double, Double)

  val parser = new OptionParser()
  var optionSet: OptionSet = _
  var testName: String = _

  var intOptions: Seq[(String, String)] = Seq(NUM_TRIALS, INTER_TRIAL_WAIT, NUM_PARTITIONS, RANDOM_SEED,
    NUM_ITERATIONS)

  var doubleOptions: Seq[(String, String)] = Seq(REGULARIZATION)
  var longOptions: Seq[(String, String)] = Seq()

  val stringOptions: Seq[(String, String)] = Seq()
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

  def intOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[Int]

  def stringOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[String]

  def booleanOptionValue(option: (String, String)) = optionSet.has(option._1)

  def doubleOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[Double]

  def longOptionValue(option: (String, String)) = optionSet.valueOf(option._1).asInstanceOf[Long]
}
