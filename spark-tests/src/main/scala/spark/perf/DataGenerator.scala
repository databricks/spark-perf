package spark.perf

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.util.Random

object DataGenerator {
  def generatePaddedString(length: Int, maxValue: Int, r: Random): String = {
    val fmtString = "%%0%sd".format(length)
    fmtString.format(r.nextInt(maxValue))
  }

  /** Creates and materializes a (K, V) dataset according to the supplied parameters. */
  def createKVDataSet(
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
  : RDD[(String, String)] = {

    val recordsPerPartition = (numRecords / numPartitions.toDouble).toInt

    def generatePartition(index: Int) = {
      // Use per-partition seeds to avoid having identical data at all partitions
      val effectiveSeed = (randomSeed ^ index).toString.hashCode
      val r = new Random(effectiveSeed)
      (1 to recordsPerPartition).map{i =>
        val key = generatePaddedString(keyLength, uniqueKeys, r)
        val value = generatePaddedString(valueLength, uniqueValues, r)
        (key, value)
      }.iterator
    }

    val inputRDD = sc.parallelize(Seq(), numPartitions).mapPartitionsWithIndex{case (index, n) => {
      generatePartition(index)
    }}

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
