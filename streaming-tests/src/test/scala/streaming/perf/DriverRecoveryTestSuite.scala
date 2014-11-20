package streaming.perf

import akka.actor.ActorSystem
import akka.pattern.ask
import org.scalatest._
import org.scalatest.concurrent.Eventually
import scala.concurrent.Await

import org.scalatest.FunSuite
import streaming.perf.util.Utils._
import scala.concurrent.duration._

class DriverRecoveryTestSuite extends FunSuite with Eventually with BeforeAndAfter {
  var appActorSystem: ActorSystem = null
  var testActorSystem: ActorSystem = null

  after {
    if (testActorSystem != null) {
      testActorSystem.shutdown()
    }
    if (appActorSystem != null) {
      appActorSystem.shutdown()
    }
  }

  test("Actor connectivity") {
    val askTimeout = 100 milliseconds
    val test = new DriverRecoveryTest
    val (system1, testActor, testActorUrl) = DriverRecoveryTestActor.createActor(test)
    testActorSystem = system1
    assert(Await.result(testActor.ask(Ready)(askTimeout).mapTo[Boolean], askTimeout))
    println("Test actor ready")

    val (system2, appActor) = DriverRecoveryTestAppActor.createActor(testActorUrl)
    appActorSystem = system2
    assert(Await.result(appActor.ask(Ready)(askTimeout).mapTo[Boolean], askTimeout))
    println("App actor ready")

    eventually(timeout(2 seconds), interval(100 milliseconds)) {
      assert(test.isRunning(), "App actor has not registered with test actor")
    }
  }
}
