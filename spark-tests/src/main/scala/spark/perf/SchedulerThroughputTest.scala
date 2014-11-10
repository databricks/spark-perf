package spark.perf

import scala.collection.JavaConverters._

import joptsimple.{OptionSet, OptionParser}
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import org.apache.spark.SparkContext

/** Schedule a large number of null tasks to test scheduler/task launching throughput. */
class SchedulerThroughputTest(sc: SparkContext) extends PerfTest {
  val NUM_TRIALS = ("num-trials", "number of trials to run")
  val INTER_TRIAL_WAIT = ("inter-trial-wait",   "seconds to sleep between trials")
  val NUM_TASKS =  ("num-tasks", "number of tasks to create/run")

  val parser = new OptionParser()
  var optionSet: OptionSet = _

  Seq(NUM_TRIALS, INTER_TRIAL_WAIT, NUM_TASKS).map{case (opt, desc) =>
    parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Int]).required()}

  def initialize(args: Array[String]) = {
    optionSet = parser.parse(args.toSeq: _*)
  }

  def createInputData() = {}

  def run(): (JValue, Seq[JValue]) = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val interTrialWait = optionSet.valueOf(INTER_TRIAL_WAIT._1).asInstanceOf[Int]
    val numTasks = optionSet.valueOf(NUM_TASKS._1).asInstanceOf[Int]

    val options: Map[String, String] = optionSet.asMap().asScala.flatMap { case (spec, values) =>
      if (spec.options().size() == 1 && values.size() == 1) {
        Some((spec.options().iterator().next(), values.iterator().next().toString))
      } else {
        None
      }
    }.toMap

    val results: Seq[JValue] = (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      sc.makeRDD(1 to numTasks, numTasks).count
      val end = System.currentTimeMillis()
      val time = (end - start).toDouble / 1000.0
      System.gc()
      Thread.sleep(interTrialWait * 1000)
      ("time" -> time) : JValue
    }

    (options, results)
  }
}
