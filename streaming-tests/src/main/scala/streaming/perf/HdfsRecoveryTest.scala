package streaming.perf

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.{Milliseconds, Time}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import streaming.perf.util.FileGenerator

class HdfsRecoveryTest extends PerfTest {
  val RECORDS_PER_FILE = ("records-per-file", "Number records per file")
  val FILE_CLEANER_DELAY = ("file-cleaner-delay", "Delay (secs) in cleaning up generated files")

  override def longOptions = super.longOptions ++ Seq(RECORDS_PER_FILE, FILE_CLEANER_DELAY)

  override def stringOptions = super.stringOptions ++ Seq(HDFS_URL)

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): String = {
    // Define variables
    val maxRecordsPerFile = longOptionValue(RECORDS_PER_FILE)
    val cleanerDelay = longOptionValue(FILE_CLEANER_DELAY)
    val dataDirectory = hdfsUrl + "/data/"
    val tempDataDirectory = hdfsUrl + "/temp/"

    // Create the file generator
    val fileGenerator = new FileGenerator(dataDirectory, tempDataDirectory, maxRecordsPerFile, cleanerDelay)
    fileGenerator.initialize()

    // Setup computation
    val fileStream = ssc.textFileStream(dataDirectory)
    val updateFunc = (values: Seq[Long], state: Option[Long]) => {
      Some(values.foldLeft(0L)(_ + _) + state.getOrElse(0L))
    }
    val wordStream = fileStream.flatMap(_.split(" ")).map(x => (x, 1L))
    val runningCountStream = wordStream.updateStateByKey[Long](updateFunc).persist(StorageLevel.MEMORY_AND_DISK_SER)
    runningCountStream.checkpoint(Milliseconds(batchDurationMs * 5))

    // Verify the running counts. For any key the running count should be in the sequence
    // 1, 3, 6, 10, 15, 21, ... (i.e., nth number is sum of 1..n)
    val expectedCounts = (1L to maxRecordsPerFile).map(x => (1L to x).reduce(_ + _)).toSet
    wordStream.foreachRDD((rdd: RDD[(String, Long)], time: Time) => {
      val partitionCounts = rdd.sparkContext.runJob(rdd.mapPartitions(iter => 
          iter.toSeq.groupBy(_._1).toSeq.map(x => (x._1, x._2.map(_._2).sum)).toIterator
          ), (iter: Iterator[(String, Long)]) => iter.toArray)
      println(s"New partition counts ${partitionCounts.size}) at $time = " +
        partitionCounts.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]"))
      val counts = rdd.reduceByKey(_ + _, 1).collect()
      println(s"New total count at $time = " + counts.mkString("[", ", ", "]"))
    })
    runningCountStream.foreachRDD((rdd: RDD[(String, Long)], time: Time) => {
      val counts = rdd.collect()
      val possibleCounts = expectedCounts
      val expected = counts.forall { case (word, count) => possibleCounts.contains(count) }
      println("Running counts at " + time + " = " + counts.mkString("[", ", ", "]") + (if (!expected) ", no match" else ""))
      println("-" * 40)
    })

    // Run the computation
    ssc.start()
    fileGenerator.start()
    Thread.sleep(totalDurationSec * 1000)
    fileGenerator.stop()
    Thread.sleep(batchDurationMs * 2)
    ssc.stop()
    Thread.sleep(100)
    fileGenerator.cleanup()
    "PASSED"
  }
}
