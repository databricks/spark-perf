package spark.perf

import spark.{SparkContext, RDD}
import spark.SparkContext._

import joptsimple.{OptionSet, OptionParser}
import java.util.Random

/** Parent class for tests which run on a large (key, value) dataset. */
abstract class KVDataTest(sc: SparkContext) extends PerfTest {
  // TODO (pwendell):
  //   - Support skewed distribution of groups
  //   - Throw error of number of unique keys/values inconsistent with number of characters

  abstract def runTest(rdd: RDD[(String, String)], reduceTasks: Int)

  val NUM_TRIALS =    ("num-trials",    "number of trials to run", 5)
  val REDUCE_TASKS =  ("reduce-tasks",  "number of reduce tasks", 100)
  val NUM_RECORDS =   ("num-records",   "number of input pairs", 100000)
  val UNIQUE_KEYS =   ("unique-keys",   "(approx) number of unique keys", 10000)
  val KEY_LENGTH =    ("key-length",    "lenth of keys in characters", 100)
  val UNIQUE_VALUES = ("unique-values", "(approx) number of unique values per key", 100)
  val VALUE_LENGTH =  ("value-length",  "length of values in characters", 100)
  val NUM_PARTITIONS =   ("num-partitions", "number of input partitions", 10000)
  val RANDOM_SEED =      ("random-seed", "seed for random number generator", new Random().nextInt)
  val PERSISTENCE_TYPE = ("persistent-type", "input persistence (memory, disk, HDFS)", "memory")

  val intOptions = Seq(NUM_TRIALS, REDUCE_TASKS, KEY_LENGTH, VALUE_LENGTH, UNIQUE_KEYS, NUM_RECORDS,
    NUM_PARTITIONS, RANDOM_SEED)
  val stringOptions = Seq(PERSISTENCE_TYPE)
  val options = intOptions ++ stringOptions

  val parser = new OptionParser()
  var optionSet: OptionSet = _
  var rdd: RDD[(String, String)] = _

  intOptions.map{case (opt, desc, default) =>
    parser.accepts(opt, desc).withOptionalArg().ofType(classOf[Int]).defaultsTo(default)
  }
  stringOptions.map{case (opt, desc, default) =>
    parser.accepts(opt, desc).withOptionalArg().ofType(classOf[String]).defaultsTo(default)
  }

  override def initialize(args: Array[String]) = {
    optionSet = parser.parse(args.toSeq: _*)
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

    if (uniqueKeys.toString.length < keyLength) throw new Exception(
      "Can't pack %s unique keys into %s digits".format(uniqueKeys, keyLength))

    if (uniqueValues.toString.length < valueLength) throw new Exception(
      "Can't pack %s unique values into %s digits".format(uniqueValues, valueLength))

    DataGenerator.createKVDataSet(sc, numRecords, uniqueKeys, keyLength, uniqueValues, valueLength,
      numPartitions, randomSeed, persistenceType)
  }

  override def getParams = options.map(_._1).map(o => (o, optionSet.valueOf(o).toString))

  override def run(): Seq[Double] = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val reduceTasks = optionSet.valueOf(REDUCE_TASKS._1).asInstanceOf[Int]

    (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      runTest(rdd, reduceTasks)
      val end = System.currentTimeMillis()
      (end - start).toDouble / 1000.0
    }
  }
}