package streaming.perf

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Milliseconds, Time}

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import streaming.perf.util.FileGenerator


class HdfsRecoveryTest extends PerfTest {
  import HdfsRecoveryTest._

  override def intOptions = super.intOptions ++ Seq(TOTAL_DURATION, RECORDS_PER_FILE, FILE_CLEANER_DELAY)

  override def stringOptions = super.stringOptions ++ Seq(HDFS_URL)

  /** Runs the test and returns a series of results, along with values of any parameters */
  def run(): String = {
    // Define variables
    val hdfsUrl = stringOptionValue(HDFS_URL)
    val maxRecordsPerFile = intOptionValue(RECORDS_PER_FILE)
    val totalDuration = intOptionValue(TOTAL_DURATION)
    val cleanerDelay = intOptionValue(FILE_CLEANER_DELAY)
    val dataDirectory = hdfsUrl + "/data/"
    val checkpointDirectory = hdfsUrl + "/checkpoint/"

    // Create the file generator
    val fileGenerator = new FileGenerator(ssc.sparkContext, dataDirectory, maxRecordsPerFile, cleanerDelay)
    fileGenerator.initialize()
    Thread.sleep(batchDuration * 2) // ensures mod time of this file is such that file stream never finds it

    // Setup computation
    val fileStream = ssc.textFileStream(dataDirectory)
    val updateFunc = (values: Seq[Long], state: Option[Long]) => {
      Some(values.foldLeft(0L)(_ + _) + state.getOrElse(0L))
    }
    val wordStream = fileStream.flatMap(_.split(" ")).map(x => (x, 1L))
    val runningCountStream = wordStream.updateStateByKey[Long](updateFunc).persist(StorageLevel.MEMORY_AND_DISK_SER)
    runningCountStream.checkpoint(Milliseconds(batchDuration * 5)

    // Verify the running counts. For any key the running count should be in the sequence
    // 1, 3, 6, 10, 15, 21, ... (i.e., nth number is sum of 1..n)
    val expectedCounts = (1 to maxRecordsPerFile).map(x => (1L to x.toLong).reduce(_ + _)).toSet
    runningCountStream.foreach((rdd: RDD[(String, Long)], time: Time) => {
      val counts = rdd.collect()
      val possibleCounts = expectedCounts
      println("Running counts at " + time + " = " + counts.mkString("[", ", ", "]"))
      val expected = counts.forall { case (word, count) => possibleCounts.contains(count) }
      if (!expected) {
        println("Running count at " + time + " did not match")
        System.exit(0)
      }
    })

    // Run the computation
    ssc.checkpoint(checkpointDirectory)
    ssc.start()
    fileGenerator.start()
    Thread.sleep(totalDuration * 1000)
    fileGenerator.stop()
    Thread.sleep(batchDuration * 2)
    ssc.stop()
    System.out.flush()
    Thread.sleep(100)
    fileGenerator.cleanup()

    "PASSED"
  }
}

object HdfsRecoveryTest {
  val HDFS_URL = ("hdfs-url", "URL of the HDFS directory that is to be used for this test", true)
  val TOTAL_DURATION = ("total-duration", "Total duration of the test in seconds", true)
  val RECORDS_PER_FILE = ("records-per-file", "Number records per file", true)
  val FILE_CLEANER_DELAY = ("file-cleaner-delay", "Delay (secs) in cleaning up generated files", true)
}
