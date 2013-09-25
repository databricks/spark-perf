package spark.perf

import joptsimple.{OptionSet, OptionParser}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/** Parent class for tests which run on a large (key, value) dataset. */
abstract class KVDataTest(sc: SparkContext) extends PerfTest {
  // TODO (pwendell):
  //   - Support skewed distribution of groups

  def runTest(rdd: RDD[(String, String)], reduceTasks: Int)

  val NUM_TRIALS =       ("num-trials",    "number of trials to run")
  val REDUCE_TASKS =     ("reduce-tasks",  "number of reduce tasks")
  val NUM_RECORDS =      ("num-records",   "number of input pairs")
  val INTER_TRIAL_WAIT = ("inter-trial-wait",   "seconds to sleep between trials")
  val UNIQUE_KEYS =   ("unique-keys",   "(approx) number of unique keys")
  val KEY_LENGTH =    ("key-length",    "lenth of keys in characters")
  val UNIQUE_VALUES = ("unique-values", "(approx) number of unique values per key")
  val VALUE_LENGTH =  ("value-length",  "length of values in characters")
  val NUM_PARTITIONS =   ("num-partitions", "number of input partitions")
  val RANDOM_SEED =      ("random-seed", "seed for random number generator")
  val PERSISTENCE_TYPE = ("persistent-type", "input persistence (memory, disk)")
  val WAIT_FOR_EXIT =    ("wait-for-exit", "JVM will not exit until input is received from stdin")

  val intOptions = Seq(NUM_TRIALS, INTER_TRIAL_WAIT, REDUCE_TASKS, KEY_LENGTH, VALUE_LENGTH, UNIQUE_KEYS,
    UNIQUE_VALUES, NUM_RECORDS, NUM_PARTITIONS, RANDOM_SEED)
  val stringOptions = Seq(PERSISTENCE_TYPE)
  val booleanOptions = Seq(WAIT_FOR_EXIT)
  val options = intOptions ++ stringOptions  ++ booleanOptions

  val parser = new OptionParser()
  var optionSet: OptionSet = _
  var rdd: RDD[(String, String)] = _

  intOptions.map{case (opt, desc) =>
    parser.accepts(opt, desc).withRequiredArg().ofType(classOf[Int]).required()
  }
  stringOptions.map{case (opt, desc) =>
    parser.accepts(opt, desc).withRequiredArg().ofType(classOf[String]).required()
  }
  booleanOptions.map{case (opt, desc) =>
    parser.accepts(opt, desc)
  }

  var waitForExit = false

  override def initialize(args: Array[String]) = {
    optionSet = parser.parse(args.toSeq: _*)
    waitForExit = optionSet.has(WAIT_FOR_EXIT._1)
  }

  override def createInputData() = {
    val numRecords: Int = optionSet.valueOf(NUM_RECORDS._1).asInstanceOf[Int]
    val uniqueKeys: Int = optionSet.valueOf(UNIQUE_KEYS._1).asInstanceOf[Int]
    val keyLength: Int = optionSet.valueOf(KEY_LENGTH._1).asInstanceOf[Int]
    val uniqueValues: Int = optionSet.valueOf(UNIQUE_VALUES._1).asInstanceOf[Int]
    val valueLength: Int = optionSet.valueOf(VALUE_LENGTH._1).asInstanceOf[Int]
    val numPartitions: Int = optionSet.valueOf(NUM_PARTITIONS._1).asInstanceOf[Int]
    val randomSeed: Int = optionSet.valueOf(RANDOM_SEED._1).asInstanceOf[Int]
    val persistenceType: String = optionSet.valueOf(PERSISTENCE_TYPE._1).asInstanceOf[String]

    if (uniqueKeys.toString.length > keyLength) throw new Exception(
      "Can't pack %s unique keys into %s digits".format(uniqueKeys, keyLength))

    if (uniqueValues.toString.length > valueLength) throw new Exception(
      "Can't pack %s unique values into %s digits".format(uniqueValues, valueLength))

    rdd = DataGenerator.createKVDataSet(sc, numRecords, uniqueKeys, keyLength, uniqueValues,
      valueLength, numPartitions, randomSeed, persistenceType)
  }

  override def run(): Seq[Double] = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val interTrialWait: Int = optionSet.valueOf(INTER_TRIAL_WAIT._1).asInstanceOf[Int]
    val reduceTasks = optionSet.valueOf(REDUCE_TASKS._1).asInstanceOf[Int]

    val result = (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      runTest(rdd, reduceTasks)
      val end = System.currentTimeMillis()
      val time = (end - start).toDouble / 1000.0
      System.gc()
      Thread.sleep(interTrialWait * 1000)
      time
    }

    if (waitForExit) {
      System.err.println("Test is finished. To exit JVM and continue, press Enter:")
      readLine()
    }
    result
  }
}

class AggregateByKey(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[(String, String)], reduceTasks: Int) {
    rdd.map{case (k, v) => (k, v.toInt)}.reduceByKey(_ + _, reduceTasks).collect
  }
}

class SortByKey(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[(String, String)], reduceTasks: Int) {
    rdd.sortByKey(numPartitions=reduceTasks).count
  }
}

class Count(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[(String, String)], reduceTasks: Int) {
    rdd.count
  }
}

class CountWithFilter(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[(String, String)], reduceTasks: Int) {
    rdd.filter{case (k, v) => k.toInt % 2 == 1}.count
  }
}
