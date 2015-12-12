package streaming.perf

import scala.collection.mutable
import scala.concurrent.TimeoutException

import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
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

class ReceiverLoadBalanceTest extends PerfTest {

  val TOTAL_EXECUTOR_CORES = ("total-executor-cores", "the total number of executor codes")

  val NUM_EXECUTORS = ("num-executors", "number of executors")

  override def longOptions = super.longOptions ++ Seq(TOTAL_EXECUTOR_CORES, NUM_EXECUTORS)

  override def run(): String = {
    val totalExecutorCores = longOptionValue(TOTAL_EXECUTOR_CORES).toInt
    require(totalExecutorCores > 0)

    val numExecutors = longOptionValue(NUM_EXECUTORS).toInt
    require(numExecutors > 0)

    if (ssc != null) {
      ssc.stop()
    }

    for (numReceivers <- 1 to totalExecutorCores) {
      println(s"Testing $numReceivers receivers")
      runTest(numReceivers, totalExecutorCores, numExecutors)
    }
    "SUCCESS"
  }

  def runTest(numReceivers: Int, totalExecutorCores: Int, numExecutors: Int): Unit = {
    require(numReceivers > 0)
    val ssc = createContext()
    try {
      val receiverLocations = new mutable.HashMap[Int, String] with mutable.SynchronizedMap[Int, String]
      ssc.addStreamingListener(new StreamingListener {
        override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
          receiverLocations(receiverStarted.receiverInfo.streamId) = receiverStarted.receiverInfo.location
        }

        override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
          receiverLocations -= receiverError.receiverInfo.streamId
        }

        override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
          receiverLocations -= receiverStopped.receiverInfo.streamId
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

      def getExecutorSize: Int = {
        SparkEnv.get.blockManager.master.getMemoryStatus.filter { case (blockManagerId, _) =>
          // Ignore the driver location
          blockManagerId.executorId != "driver" && blockManagerId.executorId != "<driver>"
        }.size
      }
      // We need to wait for all executors up so as to verify the scheduling policy works correctly
      val startTime = System.currentTimeMillis()
      val timeoutForExecutors = 30000 // 30 seconds
      var executorSize = getExecutorSize
      while (executorSize < numExecutors) {
        if (System.currentTimeMillis() < startTime + timeoutForExecutors) {
          Thread.sleep(100)
        } else {
          throw new TimeoutException(s"Cannot enough executors (expected $numExecutors, " +
            s"but was $executorSize) online before starting StreamingContext in $timeoutForExecutors milliseconds")
        }
        executorSize = getExecutorSize
      }

      ssc.start()

      // Assume all receivers should start in totalDurationSec seconds
      val streamingStartTime = System.currentTimeMillis()
      val timeout = totalDurationSec * 1000
      while (receiverLocations.size < numReceivers) {
        if (System.currentTimeMillis() < streamingStartTime + timeout) {
          Thread.sleep(100)
        } else {
          throw new AssertionError(s"$numReceivers receivers cannot start in ${totalDurationSec} seconds")
        }
      }
      val locationsToNumReceivers = receiverLocations.groupBy(_._2).map { case (location, values) =>
        (location, values.size)
      }
      val minNumReceivers = locationsToNumReceivers.values.min
      val maxNumReceivers = locationsToNumReceivers.values.min
      // If it's balanced, maxNumReceivers - minNumReceivers should <= 1
      if (maxNumReceivers - minNumReceivers > 1) {
        throw new AssertionError(s"Receivers are not balanced: $receiverLocations $locationsToNumReceivers")
      }
      if (numReceivers >= numExecutors && locationsToNumReceivers.size < numExecutors) {
        // There are more receivers than executors, but the number of locations is less than the number of executors.
        // It means there are some idle executors and we should fail the test.
        throw new AssertionError(s"Receivers are not balanced: $receiverLocations $locationsToNumReceivers")
      }
    } finally {
      ssc.stop(true, true)
    }
  }
}

/**
 * A receiver that will fail until `now` is greater than `failUtil`.
 */
class FailureReceiver(failUtil: Long) extends Receiver[Int](StorageLevel.MEMORY_ONLY) {

  @volatile private var stopped = false

  override def onStart(): Unit = {
    stopped = false
    new Thread("FailureReceiver") {
      override def run(): Unit = {
        if (System.currentTimeMillis() < failUtil) {
          throw new RuntimeException("Oops")
        }
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

/**
 * Run some receivers that keeps failing and test if we can recovery them when they become stable.
 */
class ReceiverFailureTest extends PerfTest {

  val NUM_RECEIVERS = ("num-receivers", "number of receivers")

  override def longOptions = super.longOptions ++ Seq(NUM_RECEIVERS)

  override def run(): String = {
    val numReceivers = longOptionValue(NUM_RECEIVERS).toInt
    require(numReceivers > 0)

    // Make receivers fail during the first half of totalDurationSec
    val failureDuration = totalDurationSec * 1000 / 2

    val receivers = new mutable.HashSet[Int] with mutable.SynchronizedSet[Int]
    ssc.addStreamingListener(new StreamingListener {
      override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = {
        receivers += receiverStarted.receiverInfo.streamId
      }

      override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = {
        receivers -= receiverError.receiverInfo.streamId
      }

      override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = {
        receivers -= receiverStopped.receiverInfo.streamId
      }
    })

    var stream: DStream[Int] = null
    var i = 0
    val now = System.currentTimeMillis()
    while (i < numReceivers) {
      val receiver = ssc.receiverStream(new FailureReceiver(now + failureDuration))
      stream = if (stream == null) receiver else stream.union(receiver)
      i += 1
    }
    stream.print()
    ssc.start()
    // Assume all receivers should start in totalDurationSec seconds
    ssc.awaitTermination(totalDurationSec * 1000)

    if (receivers.size != numReceivers) {
      throw new AssertionError(s"There is only ${receivers.size} remaining. Cannot recovery to $numReceivers")
    }
    ssc.stop()

    "SUCCESS"
  }
}


/**
 * Run receivers more than executors and see if we can stop StreamingContext.
 */
class ReceiverTimeoutTest extends PerfTest {

  val TOTAL_EXECUTOR_CORES = ("total-executor-cores", "the total number of executor codes")

  val RECEIVER_TIMEOUT = ("receiver-timeout", "timeout to launch a receiver")

  override def longOptions = super.longOptions ++ Seq(TOTAL_EXECUTOR_CORES, RECEIVER_TIMEOUT)

  override def run(): String = {
    if (ssc != null) {
      ssc.stop()
    }

    val totalExecutorCores = longOptionValue(TOTAL_EXECUTOR_CORES).toInt
    require(totalExecutorCores > 0)

    val receiverTimeout = longOptionValue(RECEIVER_TIMEOUT)

    val conf = new SparkConf().setAppName(testName).
      set("spark.streaming.receiver.launching.timeout", receiverTimeout + "ms")
    val sparkContext = new SparkContext(conf)
    ssc = new StreamingContext(sparkContext, Milliseconds(batchDurationMs))

    val numReceivers = totalExecutorCores + 1
    var stream: DStream[Int] = null
    var i = 0
    val now = System.currentTimeMillis()
    while (i < numReceivers) {
      val receiver = ssc.receiverStream(new BasicReceiver)
      stream = if (stream == null) receiver else stream.union(receiver)
      i += 1
    }
    stream.print()
    ssc.start()
    try {
      ssc.awaitTermination(receiverTimeout * 2)
      throw new AssertionError("There are not enough executor cores for receivers but TimeoutException is not thrown ")
    } catch {
      case e: TimeoutException => // This is expected
    }

    "SUCCESS"
  }
}

