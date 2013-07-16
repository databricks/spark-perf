package spark.perf

import spark.{SparkContext, RDD}
import spark.SparkContext._

import joptsimple.{OptionSet, OptionParser}

/** Parent class for tests which run on a large (key, value) dataset. */
abstract class KVDataTest(sc: SparkContext) extends PerfTest {
  // TODO:
  //   - Random seeds that are hashed with each partition ID
  //   - Support skewed distribution of groups
  //   - Throw error of number of unique keys/values inconsistent with number of characters

  abstract def runTest(rdd: RDD[(String, String)])

  val NUM_TRIALS =   ("num-trials",   "number of trials to run", 5)
  val REDUCE_TASKS = ("reduce-tasks", "number of reduce tasks", 100)
  val KEY_LENGTH =   ("key-length",   "lenth of keys in characters", 100)
  val VALUE_LENGTH = ("value-length", "length of values in characters", 100)
  val NUM_KEYS =     ("num-keys",     "(approx) number of unique keys", 10000)
  val NUM_VALUES =   ("num-values",   "(approx) number of unique values per key", 100)
  val NUM_RECORDS =  ("num-records",  "number of input pairs", 100000)
  val NUM_PARTITIONS =   ("num-partitions",  "number of input partitions", 10000)
  val PERSISTENCE_TYPE = ("persistent-type", "input persistence (memory, disk, HDFS)", "memory")

  val intOptions = Seq(
    NUM_TRIALS, REDUCE_TASKS, KEY_LENGTH, VALUE_LENGTH, NUM_KEYS, NUM_RECORDS, NUM_PARTITIONS)
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
    val numKeys: Int = optionSet.valueOf(NUM_KEYS._1).asInstanceOf[Int]
    val numValues: Int = optionSet.valueOf(NUM_VALUES._1).asInstanceOf[Int]
    val numPartitions: Int = optionSet.valueOf(NUM_PARTITIONS._1).asInstanceOf[Int]
    val keyLength: Int = optionSet.valueOf(KEY_LENGTH._1).asInstanceOf[Int]
    val valueLength: Int = optionSet.valueOf(VALUE_LENGTH._1).asInstanceOf[Int]
    val numRecords: Int = optionSet.valueOf(NUM_RECORDS._1).asInstanceOf[Int]
    val persistenceType: String = optionSet.valueOf(PERSISTENCE_TYPE._1).asInstanceOf[String]

    rdd = DataGenerator.createKVDataSet(sc, numKeys, numValues, numRecords, keyLength,
      valueLength, numPartitions, persistenceType)
  }

  override def getParams = options.map(_._1).map(o => (o, optionSet.valueOf(o).toString))

  override def run(): Seq[Double] = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val reduceTasks = optionSet.valueOf(REDUCE_TASKS._1).asInstanceOf[Int]

    (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      runTest(rdd)
      val end = System.currentTimeMillis()
      (end - start).toDouble / 1000.0
    }
  }
}