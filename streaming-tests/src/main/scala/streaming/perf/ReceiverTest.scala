package streaming.perf

import java.util.concurrent.{TimeUnit, CountDownLatch}

import scala.concurrent.TimeoutException

import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler._

/**
 * Try different numbers of receivers and test if we can distribute them evenly.
 */
class BasicReceiver extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  @volatile private var stopped = false

  override def onStart(): Unit = {
    stopped = false
    new Thread("BasicReceiver") {
      override def run(): Unit = {
        while (!stopped) {
          Thread.sleep(100)
        }
      }
    }.start()
  }

  override def onStop(): Unit = {
    stopped = true
  }
}

class ReceiverTest(sc: SparkContext) extends PerfTest(sc) {

  val NUM_RECEIVERS = ("num-receivers", "number of receivers")

  override def longOptions = super.longOptions ++ Seq(NUM_RECEIVERS)

  override def run(): String = {
    doRunPerf()
    "PASSED"
  }

  override def doRunPerf(): Seq[(String, Double)] = {
    val ssc = createContext()
    val numReceivers = longOptionValue(NUM_RECEIVERS).toInt

    val allReceiversStartLatch = new CountDownLatch(numReceivers)

    ssc.addStreamingListener(new StreamingListener {
      override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
        allReceiversStartLatch.countDown()
      }
    })

    var stream: DStream[Int] = null
    var i = 0
    while (i < numReceivers) {
      val receiver = ssc.receiverStream(new BasicReceiver)
      stream = if (stream == null) receiver else stream.union(receiver)
      i += 1
    }
    stream.print()
    val start = System.currentTimeMillis()
    ssc.start()
    try {
      if (allReceiversStartLatch.await(120, TimeUnit.SECONDS)) {
        Seq("receivers-start-time" -> (System.currentTimeMillis() - start) / 1000.0)
      } else {
        throw new TimeoutException("Receivers cannot start in 2 minutes")
      }
    } finally {
      ssc.stop(stopSparkContext = false, stopGracefully = true)
    }
  }
}
