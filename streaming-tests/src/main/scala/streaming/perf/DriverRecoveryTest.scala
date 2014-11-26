package streaming.perf

import java.io.File

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

import akka.actor._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.deploy.Client
import streaming.perf.util._

sealed trait DriverRecoveryTestMessage extends Serializable
object Ready extends DriverRecoveryTestMessage
case class RegisterApp(appActor: ActorRef) extends DriverRecoveryTestMessage
object DeregisterApp extends DriverRecoveryTestMessage
case class GotCounts(time: Long, counts: Array[(String, Long)]) extends DriverRecoveryTestMessage
case class KillApp(exitCode: Int) extends DriverRecoveryTestMessage

class DriverRecoveryTestActor(recoveryTest: DriverRecoveryTest) extends Actor {

  var appActor: ActorRef = null

  def receive = {
    case RegisterApp(actor) =>
      recoveryTest.onRegister()
      appActor = actor
      sender ! true

    case DeregisterApp =>
      recoveryTest.onDeregister()
      appActor = null
      sender ! true

    case GotCounts(time, counts) =>
      recoveryTest.onGotCounts(time, counts)

    case KillApp(exitCode) =>
      if (appActor != null) {
        appActor ! KillApp(exitCode)
      }

    case Ready =>
      sender ! true
  }
}

object DriverRecoveryTestActor {
  val actorName = "TestActor"
  val actorSystemName = "DriverRecoveryTest"

  def createActor(recoveryTest: DriverRecoveryTest): (ActorSystem, ActorRef, String) = {
    val (actorSystem, actorSystemPort, testActor) = Utils.createActorSystemAndActor(
      actorSystemName, actorName, Props(new DriverRecoveryTestActor(recoveryTest)))
    val testActorUrl =
      s"akka.tcp://${actorSystem.name}@${Utils.localIpAddress}:$actorSystemPort/user/$actorName"
    (actorSystem, testActor, testActorUrl)
  }
}

class DriverRecoveryTest extends PerfTest {

  private val USE_RECEIVER = ("use-receiver", "whether to use receiver to generate data")

  override def booleanOptions = super.booleanOptions ++ Seq(USE_RECEIVER)

  private val APP_START_WAIT_TIMEOUT = 60 * 1000
  private val MIN_APP_KILL_INTERVAL = 60 * 1000
  private val MAX_APP_KILL_INTERVAL = 90 * 1000
  private val RECOVERY_WAIT_TIMEOUT = 120 * 1000

  private val RECORDS_PER_FILE = 10000
  private val FILE_CLEANER_DELAY = 300

  private val master = new SparkConf().get("spark.master", "local[4]")
  private val standaloneModeDriverLauncherClass = Client.getClass.getName.stripSuffix("$")

  protected val registerTimes = new ArrayBuffer[Long]
  protected val deregisterTimes = new ArrayBuffer[Long]
  protected val upTimes = new ArrayBuffer[Long]
  protected val downTimes = new ArrayBuffer[Long]

  protected var expectedCounts: Set[Long] = Set.empty

  protected var hasStopped: Boolean = false
  protected var hasStartedOrRestarted: Boolean = false
  protected var hasGotFirstCount: Boolean = false
  protected var hasCountsBeenGeneratedAfterStart: Boolean = false
  protected var hasCountsMatched: Boolean = true
  protected var lastStartTime: Long = -1
  protected var lastStopTime: Long = -1

  override def run(): String = {
    val useReceiver = booleanOptionValue(USE_RECEIVER)
    var fileGenerator: FileGenerator = null

      // Clear earlier checkpoints
    val checkpointPath = new Path(checkpointDirectory)
    val fs = checkpointPath.getFileSystem(new Configuration())
    if (fs.exists(checkpointPath)) {
      fs.delete(checkpointPath, true)
      log("Deleted " + checkpointDirectory)
    }

    // Create the actor for communication
    log("Creating actor")
    val (actorSystem, testActor, testActorUrl) = DriverRecoveryTestActor.createActor(this)
    log(s"Created actor system ${actorSystem.name}, and " +
      s"test actor $testActor with url $testActorUrl")

    // Create the file generator if not using receivers
    val dataDirectory = hdfsUrl + "/data/"
    val tempDataDirectory = hdfsUrl + "/temp/"

    if (!useReceiver) {
      log("Creating file generator")
      fileGenerator = new FileGenerator(dataDirectory, tempDataDirectory,
        RECORDS_PER_FILE, 2 * batchDurationMs.toInt, FILE_CLEANER_DELAY)
      fileGenerator.initialize()
      expectedCounts = (1L to RECORDS_PER_FILE).map(x => (1L to x).reduce(_ + _)).toSet
      log(s"Created file generator in $dataDirectory")
    }

    log("Launching app")
    val appClass = DriverRecoveryTestApp.getClass.getName.stripSuffix("$")
    val appArgs = Seq(master, batchDurationMs.toString, checkpointDirectory,
      testActorUrl, useReceiver.toString, dataDirectory)
    val appDriverId = launchAppAndGetDriverId(appClass, appArgs)

    try {
      // Start the generator after a delay
      log("Waiting until first count is received")
      Utils.waitUntil(() => hasGotFirstCount, APP_START_WAIT_TIMEOUT,
        s"App has not generated results even after waiting for $APP_START_WAIT_TIMEOUT millis"
      )

      // Start file generation
      log("Starting file generator")
      fileGenerator.start()

      // Keep sending kill messages after random intervals
      log("Starting to send kill messages after random intervals")
      val launchTime = System.currentTimeMillis
      def timeSinceLaunch = System.currentTimeMillis - launchTime
      while (timeSinceLaunch < totalDurationSec * 1000) {
        val sleepDuration =
          MIN_APP_KILL_INTERVAL + Random.nextInt(MAX_APP_KILL_INTERVAL - MIN_APP_KILL_INTERVAL)
        Thread.sleep(sleepDuration)
        killAndRecoverDriver(testActor)
      }

      // Check if counts have matched or not
      if (!hasCountsMatched) {
        throw new Exception("Counts have not matched")
      }
    } catch {
      case e: Exception =>
        println("Error in the test: ", e)
        warn("Error in the test: ", e)
        return "FAILED: " + e.getMessage
    } finally {
      stopApp(appDriverId)
      actorSystem.shutdown()
      if (fileGenerator != null) {
        fileGenerator.stop()
      }
    }

    // Print stats
    log(s"Number of times killed = ${deregisterTimes.size}")
    log(s"Average uptime = ${upTimes.sum.toDouble / upTimes.size / 1000.0} sec")
    log(s"Average downtime = ${downTimes.sum.toDouble / downTimes.size / 1000.0} sec")
    "PASSED"
  }

  /** Called when the app registered itself */
  def onRegister() = synchronized {
    if (!hasStartedOrRestarted) {
      hasStartedOrRestarted = true
      val registerTime = System.currentTimeMillis
      registerTimes += registerTime
      lastStartTime = registerTime
      log("=" * 40)
      log("App started at " + registerTime)
    } else {
      warn("App already started, cannot start again")
    }
  }

  /** Called when the app deregisters itself */
  def onDeregister() = synchronized {
    if (!hasStopped) {
      hasStopped = true
      val deregisterTime = System.currentTimeMillis
      val upTime = deregisterTime - lastStartTime
      deregisterTimes += deregisterTime
      lastStopTime = deregisterTime
      upTimes += upTime
      log("App killed at " + deregisterTime + "\nUptime was " + upTime + " ms")
      log("=" * 40)
      log("App deregistered after " + upTime + " ms")
    } else {
      warn("App already stopped, cannot stop again")
    }
  }

  /** Is the app running right now */
  def isRunning() = synchronized { hasStartedOrRestarted && !hasStopped }


  /** Called when the app reports counts */
  def onGotCounts(time: Long, counts: Array[(String, Long)]) = synchronized {
    hasGotFirstCount = true
    if (hasStartedOrRestarted) {
      if (!hasCountsBeenGeneratedAfterStart) {
        val firstCountTime  = System.currentTimeMillis
        val downTime = firstCountTime - lastStopTime
        log("First count after recovery at " + firstCountTime)
        if (lastStopTime > 0) {
          log("Downtime was " + downTime + " ms")
        }
        downTimes += downTime
        hasCountsBeenGeneratedAfterStart = true
        this.notifyAll()
      }
    } else {
      warn("Count received after kill signal but before restart. Ignored.")
    }
    verifyCounts(time, counts)
  }

  /** Launch the test streaming app through spark-submit */
  private def launchAppAndGetDriverId(driverClass: String, driverArgs: Seq[String]): String = {
    val appArgsStr = driverArgs.mkString(" ")
    val command = s"bin/spark-submit " +
      s"--master $master --deploy-mode cluster --supervise --class $driverClass file:$jarFile $appArgsStr"
    log("\tCommand: [ " + command + " ]")
    val commandResult = new ArrayBuffer[String]
    val processBuilder = Process(command, Some(new File(sparkDir)), System.getenv.toArray: _*)
    val processLogger = ProcessLogger(commandResult += _)
    val exitValue = processBuilder.run(processLogger).exitValue()
    log(s"\tCommand result: exit value = $exitValue,  output: \n" +
      s"${commandResult.map { "\t" + _ }.mkString("\n")}\n---")
    commandResult.filter(_.contains("driver-")).headOption.map(_.split(" ").last).getOrElse {
      throw new Exception("Could not get driver id after launching")
    }
  }

  /** Stop by submitting a killing request through DriverClient */
  private def stopApp(appDriverId: String) {
    val command = s"$sparkDir/bin/spark-class $standaloneModeDriverLauncherClass kill $master $appDriverId 2>&1"
    log("Command: [ " + command + " ]")
    val commandResult = command.!!
    log(s"Result:\n${commandResult.split("\n").map { "\t" + _ }.mkString("\n")}\n---")
  }

  /** Send kill signal and wait for the driver to recover and start generating counts again */
  private def killAndRecoverDriver(testActor: ActorRef) = synchronized {
    // Send kill signal
    hasStopped = false
    hasStartedOrRestarted = false
    hasCountsBeenGeneratedAfterStart = false
    testActor ! KillApp(-1)
    log("Sent kill signal")

    // wait for recovery
    this.wait(RECOVERY_WAIT_TIMEOUT)
    if (!hasStopped) {
      throw new Exception("App driver was not killed within " + RECOVERY_WAIT_TIMEOUT + " ms of kill signal")
    }
    if (!hasStartedOrRestarted) {
      throw new Exception("App driver was not restarted within " + RECOVERY_WAIT_TIMEOUT + " ms of kill signal")
    }
    if (!hasCountsBeenGeneratedAfterStart) {
      throw new Exception("App driver was not recovered as counts were not generated within " + RECOVERY_WAIT_TIMEOUT + " ms of kill signal")
    }
  }

  /** Verify the counts */
  private def verifyCounts(time: Long, counts: Array[(String, Long)]) {
    if (expectedCounts.nonEmpty) {
      val matched = counts.forall {
        case (word, count) => expectedCounts.contains(count)
      }
      hasCountsMatched &= matched
      val logStr = s"Counts at $time = ${counts.toSeq}" +
        (if (!matched) ", no match" else "")
      log(logStr + "\n" + ("-" * 40))
    } else {
      warn("Expected results not yet configured")
    }
  }

  /** Do not create a context */
  override protected def createContext() = {
    // Do not create a new StreamingContext as this class is not the streaming app. Rather it is going to
    // launch the streaming app within the Spark cluster.
    null
  }
  
  private def log(message: => String) {
    println(s"INFO: $message")
  }

  private def warn(message: => String, ex: Exception = null) {
    println(s"WARN: $message" + Option(ex).map { e => ": " + e.toString })
  }
}
