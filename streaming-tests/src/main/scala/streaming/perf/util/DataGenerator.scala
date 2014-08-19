package streaming.perf.util

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Time
import org.apache.spark.rdd.RDD
import scala.util.Random

class DataGenerator(
    @transient sparkContext: SparkContext,
    batchDurationMs: Long,
    recordsPerSec: Long,
    uniqueKeys: Long,
    uniqueValues: Long,
    streamIndex: Int
  ) extends Serializable {
  val partitions = batchDurationMs.toInt / System.getProperty("spark.streaming.blockInterval", "200").toInt
  val recordsPerPartition = (recordsPerSec.toDouble * (batchDurationMs.toDouble / 1000) / partitions).toLong
  println("Going generate RDDs with " + partitions + " partitions having " + recordsPerPartition + " records each")

  // Generates RDDs of raw data
  def generateRDD(time: Time): RDD[(String, String)] = {

    def generatePartition(partitionIndex: Int) = {
      println("Generating " + recordsPerPartition + " records for partition " + partitionIndex + " and time " + time)
      // Use per-stream, per-time and per-partition seeds to avoid having identical data
      val effectiveSeed = (streamIndex * partitionIndex * time.milliseconds).toString.hashCode
      val r = new Random(effectiveSeed)
      (1L to recordsPerPartition).map{i =>
        val key = r.nextLong % uniqueKeys
        val value = r.nextLong % uniqueValues
        (key.toString, value.toString)
      }.iterator
    }
    sparkContext.makeRDD(1 to partitions, partitions.toInt).mapPartitionsWithIndex {
      case (pIdx, iter) => generatePartition(pIdx)
    }
  }

  // Create a receiver
  def createReceiver() {

  }
}

