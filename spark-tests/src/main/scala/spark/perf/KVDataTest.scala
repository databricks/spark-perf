package spark.perf

import scala.collection.JavaConverters._

import joptsimple.{OptionSet, OptionParser}
import com.google.common.hash.Hashing
import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/** Parent class for tests which run on a large (key, value) dataset. */
abstract class KVDataTest(sc: SparkContext, dataType: String = "string") extends PerfTest {
  // TODO (pwendell):
  //   - Support skewed distribution of groups

  def runTest(rdd: RDD[_], reduceTasks: Int)

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
  val PERSISTENCE_TYPE = ("persistent-type", "input persistence (memory, disk, hdfs)")
  val STORAGE_LOCATION = ("storage-location", "directory used for storage with 'hdfs' persistence type")
  val HASH_RECORDS =     ("hash-records", "Use hashes instead of padded numbers for keys and values")
  val WAIT_FOR_EXIT =    ("wait-for-exit", "JVM will not exit until input is received from stdin")

  val intOptions = Seq(NUM_TRIALS, INTER_TRIAL_WAIT, REDUCE_TASKS, KEY_LENGTH, VALUE_LENGTH, UNIQUE_KEYS,
    UNIQUE_VALUES, NUM_RECORDS, NUM_PARTITIONS, RANDOM_SEED)
  val stringOptions = Seq(PERSISTENCE_TYPE, STORAGE_LOCATION)
  val booleanOptions = Seq(WAIT_FOR_EXIT, HASH_RECORDS)
  val options = intOptions ++ stringOptions  ++ booleanOptions

  val parser = new OptionParser()
  var optionSet: OptionSet = _
  var rdd: RDD[_] = _

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
  var hashRecords = false

  override def initialize(args: Array[String]) = {
    optionSet = parser.parse(args.toSeq: _*)
    waitForExit = optionSet.has(WAIT_FOR_EXIT._1)
    hashRecords = optionSet.has(HASH_RECORDS._1)
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
    val storageLocation: String = optionSet.valueOf(STORAGE_LOCATION._1).asInstanceOf[String]

    if (uniqueKeys.toString.length > keyLength) throw new Exception(
      "Can't pack %s unique keys into %s digits".format(uniqueKeys, keyLength))

    if (uniqueValues.toString.length > valueLength) throw new Exception(
      "Can't pack %s unique values into %s digits".format(uniqueValues, valueLength))

    val hashFunction = hashRecords match {
      case true => Some(Hashing.goodFastHash(math.max(keyLength, valueLength) * 4))
      case false => None
    }

    rdd = dataType match {
      case "string" =>
        DataGenerator.createKVStringDataSet(sc, numRecords, uniqueKeys, keyLength, uniqueValues,
          valueLength, numPartitions, randomSeed, persistenceType, storageLocation, hashFunction)
      case "int" =>
        DataGenerator.createKVIntDataSet(sc, numRecords, uniqueKeys, uniqueValues,
          numPartitions, randomSeed, persistenceType, storageLocation)
      case _ =>
        throw new IllegalArgumentException("Unknown data type: " + dataType)
    }
  }

  override def run(): (JValue, Seq[JValue]) = {
    val numTrials = optionSet.valueOf(NUM_TRIALS._1).asInstanceOf[Int]
    val interTrialWait: Int = optionSet.valueOf(INTER_TRIAL_WAIT._1).asInstanceOf[Int]
    val reduceTasks = optionSet.valueOf(REDUCE_TASKS._1).asInstanceOf[Int]

    val options: Map[String, String] = optionSet.asMap().asScala.flatMap { case (spec, values) =>
      if (spec.options().size() == 1 && values.size() == 1) {
        Some((spec.options().iterator().next(), values.iterator().next().toString))
      } else {
        None
      }
    }.toMap

    val results: Seq[JValue] = (1 to numTrials).map { t =>
      val start = System.currentTimeMillis()
      runTest(rdd, reduceTasks)
      val end = System.currentTimeMillis()
      val time = (end - start).toDouble / 1000.0
      System.gc()
      Thread.sleep(interTrialWait * 1000)
      "time" -> time : JValue
    }

    if (waitForExit) {
      System.err.println("Test is finished. To exit JVM and continue, press Enter:")
      readLine()
    }
    (options, results)
  }
}

class AggregateByKey(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(String, String)]]
      .map{case (k, v) => (k, v.toInt)}.reduceByKey(_ + _, reduceTasks).count()
  }
}

class AggregateByKeyInt(sc: SparkContext) extends KVDataTest(sc, "int") {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(Int, Int)]]
      .reduceByKey(_ + _, reduceTasks).count()
  }
}

class AggregateByKeyNaive(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(String, String)]]
      .map{case (k, v) => (k, v.toInt)}.groupByKey.map{case (k, vs) => vs.sum}.count()
  }
}

class SortByKey(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(String, String)]]
      .sortByKey(numPartitions=reduceTasks).count()
  }
}

class SortByKeyInt(sc: SparkContext) extends KVDataTest(sc, "int") {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(Int, Int)]]
      .sortByKey(numPartitions=reduceTasks).count()
  }
}

class Count(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(String, String)]]
      .count()
  }
}

class CountWithFilter(sc: SparkContext) extends KVDataTest(sc) {
  override def runTest(rdd: RDD[_], reduceTasks: Int) {
    rdd.asInstanceOf[RDD[(String, String)]]
      .filter{case (k, v) => k.toInt % 2 == 1}.count()
  }
}
