package spark.perf

import scala.collection.JavaConverters._

import joptsimple.{OptionSet, OptionParser}
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import org.apache.spark.SparkContext

import scala.util.Random

/** Schedule a large number of null tasks to test scheduler/task launching throughput. */
class SchedulerThroughputTest(sc: SparkContext) extends PerfTest {
  val NUM_TRIALS = ("num-trials", "number of trials to run")
  val INTER_TRIAL_WAIT = ("inter-trial-wait",   "seconds to sleep between trials")
  val NUM_TASKS =  ("num-tasks", "number of tasks to create/run in each job")
  val NUM_JOBS = ("num-jobs", "number of jobs to run")
  val RANDOM_SEED = ("random-seed", "seed for random number generator")
  val CLOSURE_SIZE = ("closure-size", "task closure size (in bytes)")

  val parser = new OptionParser()
  var optionSet: OptionSet = _

  Seq(NUM_TRIALS, INTER_TRIAL_WAIT, NUM_TASKS, NUM_JOBS, RANDOM_SEED, CLOSURE_SIZE).map {
    case (opt, desc) =>
      parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Int]).required()
  }

  def initialize(args: Array[String]) = {
    optionSet = parser.parse(args.toSeq: _*)
  }

  def createInputData() = {}

  def run(): (JValue, Seq[JValue]) = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val interTrialWait = optionSet.valueOf(INTER_TRIAL_WAIT._1).asInstanceOf[Int]
    val numTasks = optionSet.valueOf(NUM_TASKS._1).asInstanceOf[Int]
    val numJobs = optionSet.valueOf(NUM_JOBS._1).asInstanceOf[Int]
    val randomSeed = optionSet.valueOf(RANDOM_SEED._1).asInstanceOf[Int]
    val closureSize = optionSet.valueOf(CLOSURE_SIZE._1).asInstanceOf[Int]

    // Allows us to simulate large task closures; this consists of random bytes in order
    // to make it incompressible:

    val mapFunction = SchedulerThroughputTest.getMapFunction(closureSize, randomSeed)

    val options: Map[String, String] = optionSet.asMap().asScala.flatMap { case (spec, values) =>
      if (spec.options().size() == 1 && values.size() == 1) {
        Some((spec.options().iterator().next(), values.iterator().next().toString))
      } else {
        None
      }
    }.toMap

    val results: Seq[JValue] = (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      (1 to numJobs).foreach { _ =>
        sc.makeRDD(1 to numTasks, numTasks).mapPartitions(mapFunction).count()
      }
      val end = System.currentTimeMillis()
      val time = (end - start).toDouble / 1000.0
      System.gc()
      Thread.sleep(interTrialWait * 1000)
      ("time" -> time) : JValue
    }

    (options, results)
  }
}

object SchedulerThroughputTest {

  private def getMapFunction(closureSize: Int, randomSeed: Int) = {
    val closureData: Array[Byte] = Array.ofDim[Byte](closureSize)
    val rand = new Random(randomSeed)
    rand.nextBytes(closureData)

    def mapFunction(iter: Iterator[Int]): Iterator[Int] = {
      closureData.size  // Reference so that the random data is serialized with the task
      iter
    }
    mapFunction _
  }

}
