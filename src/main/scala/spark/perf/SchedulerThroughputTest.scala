package spark.perf

import joptsimple.{OptionSet, OptionParser}

import spark.SparkContext

/** Schedule a large number of null tasks to test scheduler/task launching throughput. */
class SchedulerThroughputTest(sc: SparkContext) extends PerfTest {
  val NUM_TRIALS = ("num-trials", "number of trials to run")
  val NUM_TASKS =  ("num-tasks", "number of tasks to create/run")

  val parser = new OptionParser()
  var optionSet: OptionSet = _

  Seq(NUM_TRIALS, NUM_TASKS).map{case (opt, desc) =>
    parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Int]).required()}

  def initialize(args: Array[String]) = {
    optionSet = parser.parse(args.toSeq: _*)
  }

  def createInputData() = {}

  def run: Seq[Double] = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val numTasks = optionSet.valueOf(NUM_TASKS._1).asInstanceOf[Int]

    (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      sc.makeRDD(1 to numTasks, numTasks).count
      val end = System.currentTimeMillis()
      (end - start).toDouble / 1000.0
    }
  }
}
