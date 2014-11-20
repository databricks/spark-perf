package streaming.perf

import akka.actor.{ActorSystem, Actor, Props}
import akka.pattern.ask

import org.scalatest.FunSuite
import streaming.perf.util.Utils._
import scala.concurrent.Await
import scala.concurrent.duration._


class UtilsSuite extends FunSuite {
  class TestActor extends Actor {
    println("Started")
    def receive = {
      case _ => sender ! true
    }
  }

  test("Actor creation") {
    val askTimeout = (100 milliseconds)
    var actorSystem: ActorSystem = null

    try {
      val (system, _, actorRef) =
        createActorSystemAndActor("System", "Actor", Props(new TestActor))
      println("Started actor")
      actorSystem = system
      Await.result(actorRef.ask(new Object)(askTimeout).mapTo[Boolean], askTimeout)
      println("Got results")
    } finally {
      actorSystem.shutdown()
    }
  }
}
