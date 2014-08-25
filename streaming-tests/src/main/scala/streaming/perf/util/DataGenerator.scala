package streaming.perf.util

import scala.util.Random

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.DStream

class DataGenerator(
    @transient streamingContext: StreamingContext,
    batchDurationMs: Long,
    recordsPerSec: Long,
    uniqueKeys: Long,
    uniqueValues: Long,
    streamIndex: Int,
    useReceiver: Boolean,
    storageLevel: StorageLevel
  ) extends Serializable with Logging {

  @transient val sparkContext = streamingContext.sparkContext

  def createInputDStream(): DStream[(String, String)] = {
    if (useReceiver) {
      streamingContext.receiverStream(createReceiver(storageLevel))
    } else {
      new CustomInputDStream[(String, String)](streamingContext, generateRDD(_))
    }
  }

  // Generates RDDs of raw data
  private def generateRDD(time: Time): RDD[(String, String)] = {
    val partitions =
      batchDurationMs.toInt / System.getProperty("spark.streaming.blockInterval", "200").toInt
    val recordsPerPartition =
      (recordsPerSec.toDouble * (batchDurationMs.toDouble / 1000) / partitions).toLong
    println("Going generate RDDs with " + partitions +
      " partitions having " + recordsPerPartition + " records each")


    def generatePartition(partitionIndex: Int) = {
      println("Generating " + recordsPerPartition +
        " records for partition " + partitionIndex + " and time " + time)
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

  // Create a receiver that generates data
  private def createReceiver(storageLevel: StorageLevel) = {
    logInfo("Creating receiver")
    new DataGeneratingReceiver(recordsPerSec, uniqueKeys, uniqueValues, storageLevel)
  }
}

