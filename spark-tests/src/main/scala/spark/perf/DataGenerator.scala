package spark.perf

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.util.Random

object DataGenerator {
  def paddedString(i: Int, length: Int): String = {
    val fmtString = "%%0%sd".format(length)
    fmtString.format(i)
  }

  /** Creates a key-value int dataset but does not cache it, allowing for subsequent processing */
  private def generateIntData(
      sc: SparkContext,
      numRecords: Int,
      uniqueKeys: Int,
      uniqueValues: Int,
      numPartitions: Int,
      randomSeed: Int)
    : RDD[(Int, Int)] =
  {
    val recordsPerPartition = (numRecords / numPartitions.toDouble).toInt

    def generatePartition(index: Int) = {
      // Use per-partition seeds to avoid having identical data at all partitions
      val effectiveSeed = (randomSeed ^ index).toString.hashCode
      val r = new Random(effectiveSeed)
      (1 to recordsPerPartition).map{i =>
        val key = r.nextInt(uniqueKeys)
        val value = r.nextInt(uniqueValues)
        (key, value)
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex{case (index, n) => {
      generatePartition(index)
    }}
  }

  /** Creates and materializes a (K, V) int dataset according to the supplied parameters. */
  def createKVIntDataSet(
      sc: SparkContext,
      numRecords: Int,
      uniqueKeys: Int,
      uniqueValues: Int,
      numPartitions: Int,
      randomSeed: Int,
      persistenceType: String,
      storageLocation: Option[String] = None)
    : RDD[(Int, Int)] =
  {
    val inputRDD = generateIntData(
      sc, numRecords, uniqueKeys, uniqueValues, numPartitions, randomSeed)

    val rdd = persistenceType match {
      case "memory" => {
        val tmp = inputRDD.persist(StorageLevel.MEMORY_ONLY)
        tmp.count()
        tmp
      }
      case "disk" => {
        val tmp = inputRDD.persist(StorageLevel.DISK_ONLY)
        tmp.count()
        tmp
      }
      case _ => {
        // TODO(pwendell) Throw exception if option not recognized
        val filename = storageLocation.getOrElse(
          "/tmp/spark-perf-%s".format(System.currentTimeMillis()))
        inputRDD.map{case (k, v) => "%s\t%s".format(k, v)}.saveAsTextFile(filename)
        sc.textFile(filename).map(s => (s.split("\t")(0).toInt, s.split("\t")(1).toInt))
      }
    }
    rdd
  }

  /** Creates and materializes a (K, V) string dataset according to the supplied parameters. */
  def createKVStringDataSet(
      sc: SparkContext,
      numRecords: Int,
      uniqueKeys: Int,
      keyLength: Int,
      uniqueValues: Int,
      valueLength: Int,
      numPartitions: Int,
      randomSeed: Int,
      persistenceType: String,
      storageLocation: Option[String] = None)
    : RDD[(String, String)] =
  {
    val ints = generateIntData(
      sc, numRecords, uniqueKeys, uniqueValues, numPartitions, randomSeed)
    val inputRDD = ints.map { case (k, v) =>
      (paddedString(k, keyLength), paddedString(v, valueLength))
    }

    val rdd = persistenceType match {
      case "memory" => {
        val tmp = inputRDD.persist(StorageLevel.MEMORY_ONLY)
        tmp.count()
        tmp
      }
      case "disk" => {
        val tmp = inputRDD.persist(StorageLevel.DISK_ONLY)
        tmp.count()
        tmp
      }
      case _ => {
        // TODO(pwendell) Throw exception if option not recognized
        val filename = storageLocation.getOrElse(
          "/tmp/spark-perf-%s".format(System.currentTimeMillis()))
        inputRDD.map{case (k, v) => "%s\t%s".format(k, v)}.saveAsTextFile(filename)
        sc.textFile(filename).map(s => (s.split("\t")(0), s.split("\t")(1)))
      }
    }
    rdd
  }
}
