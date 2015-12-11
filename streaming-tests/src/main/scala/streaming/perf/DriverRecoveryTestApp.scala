package streaming.perf

import java.io.File

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext, Time}
import org.apache.spark.streaming.StreamingContext._
import streaming.perf.util.Utils

class DriverRecoveryTestAppActor(testActorUrl: String) extends Actor {
  import DriverRecoveryTestAppActor._

  val testActor = context.system.actorSelection(testActorUrl)

  override def preStart() {
    println(s"Trying to register driver actor with $testActor")
    Await.ready(testActor.ask(RegisterApp(this.self))(timeout / 2), timeout / 2)
    println(s"Registered app actor")
  }

  def receive = {
    case Ready =>
      println("Responding to ready")
      sender ! true

    case GotCounts(time, counts) =>
      println(s"Sending counts of time $time: ${counts.toSeq}")
      testActor ! GotCounts(time, counts)
      
    case KillApp(exitCode) =>
      println(s"Kill command with exit code $exitCode received")
      Await.ready(testActor.ask(DeregisterApp)(timeout), timeout)
      println("Deregistered app actor")
      System.exit(exitCode)
      println("Not killed") // this gets printed only if the exit does not work

    case _ =>
      println("Unknown message received")
  }
}

object DriverRecoveryTestAppActor {

  val timeout = 5 seconds

  def createActor(testActorUrl: String): (ActorSystem, ActorRef) = {
    val (actorSystem, _, appActor) = Utils.createActorSystemAndActor(
      "DriverRecoveryTestApp", "AppActor", Props(new DriverRecoveryTestAppActor(testActorUrl)))
    Await.ready(appActor.ask(Ready)(timeout), timeout)
    (actorSystem, appActor)
  } 
}

object DriverRecoveryTestApp {

  @transient var appActor: ActorRef = null

  // Create a new StreamingContext
  def createContext(
      master: String,
      batchDurationMs: Long,
      checkpointPath: String,
      useReceiver: Boolean,
      optionalDataDirectory: Option[String]
    ) = {
    require(
      useReceiver || optionalDataDirectory.nonEmpty,
      "if receiver is not to be used, then data directory has to be provided"
    )
    // Create the context
    println("Creating context")
    val jarFile = new File("streaming-perf-tests-assembly.jar").getAbsolutePath
    val sparkDir = Option(System.getenv("SPARK_HOME")).getOrElse( throw new Exception("SPARK_HOME not set"))
    println("Creating streaming context with spark directory = " + sparkDir + " and jar file  = " + jarFile)
    val ssc = new StreamingContext(master, "TestRunner: DriverRecoveryTest",
      Milliseconds(batchDurationMs), sparkDir, Seq(jarFile))
    ssc.checkpoint(checkpointPath)
    // Set up the computation
    val inputStream = if (useReceiver) {
      ssc.receiverStream[String](null)
    } else {
      ssc.textFileStream(optionalDataDirectory.get)
    }
    val wordStream = inputStream.flatMap(_.split(" ")).map(x => (x, 1L))
    val updateFunc = (values: Seq[Long], state: Option[Long]) => {
      Some(values.foldLeft(0L)(_ + _) + state.getOrElse(0L))
    }
    val runningCountStream = wordStream.updateStateByKey[Long](updateFunc).persist(StorageLevel.MEMORY_AND_DISK_SER)
    runningCountStream.checkpoint(Milliseconds(batchDurationMs * 5))

    val sendCountsToTest = (rdd: RDD[(String, Long)], time: Time) => {
      val counts = rdd.collect()
      appActor ! GotCounts(time.milliseconds, counts)
    }
    runningCountStream.foreachRDD(sendCountsToTest)
    ssc
  }

  def main(args: Array[String]) {
    import java.text.SimpleDateFormat
    import java.util.Calendar
    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val cal = Calendar.getInstance()
    println("=" * 40)
    println("Started at " + dateFormat.format(cal.getTime()))
    println("Args: " + args.mkString(", "))
    if (args.size < 5) {
      println("Incorrect number of arguments")
      println(s"${this.getClass.getSimpleName.stripSuffix("$")} <master> <batch duration in ms> " +
        s"<checkpoint path> <test actor url> <whether to use receiver> " +
        s"[<data directory if receiver not used>]")
      System.exit(255)
    }
    val Array(master, batchDurationMs, checkpointPath, testActorUrl, useReceiver) = args.take(5)
    val optionalDataDirectory = Option(args.applyOrElse(5, null))
    println("Parsed args")
    appActor = DriverRecoveryTestAppActor.createActor(testActorUrl)._2
    println("Created actor")
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      createContext(master, batchDurationMs.toLong,
        checkpointPath, useReceiver.toBoolean, optionalDataDirectory)
    })
    println("Prepared context")
    ssc.start()
    println("Started")
    ssc.awaitTermination()
  }
}


