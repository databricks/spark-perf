package spark.perf

import spark.{SparkContext, RDD}
import spark.storage.StorageLevel

import java.util.Random

object DataGenerator {
  def generatePaddedString(length: Int, maxValue: Int, r: Random): String = {
    val fmtString = "%%0%sd".format(length)
    fmtString.format(r.nextInt(maxValue))
  }


  def createKVDataSet(sc: SparkContext, numKeys: Int, numValues: Int, numRecords: Int,
    keyLength: Int, valueLength: Int, numPartitions: Int, persistenceType: String)
  : RDD[(String, String)] = {
    val recordsPerPartition = (numRecords / numPartitions.toDouble).toInt
    val inputRDD = sc.parallelize(Seq(), numPartitions).mapPartitions(n => {
      val r = new Random()
      (0 to recordsPerPartition).map{i =>
        (generatePaddedString(keyLength, numKeys, r), generatePaddedString(valueLength, numValues, r))
      }.iterator
    })

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
        val filename = "/tmp/spark-perf-%s".format(System.currentTimeMillis())
        inputRDD.map{case (k, v) => "%s\t%s".format(k, v)}.saveAsTextFile(filename)
        sc.textFile(filename).map(s => (s.split("\t")(0), s.split("\t")(1)))
      }
    }
    rdd
  }
}
