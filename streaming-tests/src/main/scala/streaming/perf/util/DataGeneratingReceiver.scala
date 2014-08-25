package streaming.perf.util

import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.storage.StorageLevel
import scala.util.Random
import scala.annotation.tailrec
import org.apache.spark.Logging
import java.util.concurrent.TimeUnit._

class DataGeneratingReceiver(
    recordsPerSec: Long,
    uniqueKeys: Long,
    uniqueValues: Long,
    storageLevel: StorageLevel
  ) extends Receiver[(String, String)](storageLevel) with Logging {

  private class DataGeneratorThread extends Thread {
    private val SYNC_INTERVAL = NANOSECONDS.convert(100, MILLISECONDS)
    private var recordsWrittenSinceSync = 0L
    private var lastSyncTime = System.nanoTime

    override def run() {
      val effectiveSeed = streamId
      val r = new Random(effectiveSeed)
      logInfo("Generating with seed " + r)
      while (!isStopped()) {
        waitToWrite()
        val key = r.nextLong % uniqueKeys
        val value = r.nextLong % uniqueValues
        store((key.toString, value.toString))
        recordsWrittenSinceSync += 1
      }
    }

    @tailrec
    private def waitToWrite() {
      val now = System.nanoTime
      val elapsedNanosecs = math.max(now - lastSyncTime, 1)
      val rate = recordsWrittenSinceSync.toDouble * 1000000000 / elapsedNanosecs
      if (rate < recordsPerSec) {
        // It's okay to write; just update some variables and return
        if (now > lastSyncTime + SYNC_INTERVAL) {
          // Sync interval has passed; let's resync
          val actualRate = recordsWrittenSinceSync.toDouble / MILLISECONDS.convert(SYNC_INTERVAL, NANOSECONDS) * 1000.0
          logInfo("Generated data at "  + actualRate + " records / sec")
          lastSyncTime = now
          recordsWrittenSinceSync = 0
        }
      } else {
        // Calculate how much time we should sleep to bring ourselves to the desired rate.
        val targetTimeInMillis = recordsWrittenSinceSync * 1000 / recordsPerSec
        val elapsedTimeInMillis = elapsedNanosecs / 1000000
        val sleepTimeInMillis = targetTimeInMillis - elapsedTimeInMillis
        if (sleepTimeInMillis > 0) {
          logTrace("Natural rate is " + rate + " per second but desired rate is " +
            recordsPerSec + ", sleeping for " + sleepTimeInMillis + " ms to compensate.")
          Thread.sleep(sleepTimeInMillis)
        }
        if (!isStopped()) {
          waitToWrite()
        }
      }
    }
  }

  def onStart() {
    val thread = new DataGeneratorThread
    thread.setDaemon(true)
    thread.start()
    logInfo("Started")
  }

  def onStop() { }
}
