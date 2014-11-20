package streaming.perf.util

import java.net.{Inet4Address, InetAddress, NetworkInterface}

import scala.collection.JavaConversions._

import akka.actor.{ActorRef, ActorSystem, ExtendedActorSystem, Props}
import com.typesafe.config.ConfigFactory

object Utils {
  lazy val localIpAddress = findLocalIpAddress()

  def findLocalIpAddress(): String = {
    val address = InetAddress.getLocalHost
    if (address.isLoopbackAddress) {
      // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
      // a better address using the local network interfaces
      // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
      // on unix-like system.
      val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.toList
      for (ni <- activeNetworkIFs) {
        for (addr <- ni.getInetAddresses if !addr.isLinkLocalAddress &&
          !addr.isLoopbackAddress && addr.isInstanceOf[Inet4Address]) {
          // We've found an address that looks reasonable!
          println("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
            " a loopback address: " + address.getHostAddress + "; using " + addr.getHostAddress +
            " instead (on interface " + ni.getName + ")")
          return addr.getHostAddress
        }
      }
      println("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
        " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
        " external IP address!")
    }
    address.getHostAddress
  }

  def createActorSystemAndActor(
      actorSystemName: String,
      actorName: String,
      actorProps: Props
    ): (ActorSystem, Int, ActorRef) = {
    var actorSystem: ActorSystem = null
    var actorSystemPort: Int = - 1
    var actorRef: ActorRef = null

    try {
      // Starting actor systems
      val port = 0
      val akkaConf = ConfigFactory.parseString(s"""
      |akka.daemonic = on
      |akka.loggers = [""akka.event.slf4j.Slf4jLogger""]
      |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      |akka.remote.netty.tcp.transport-class = "akka.remote.transport.netty.NettyTransport"
      |akka.remote.netty.tcp.port = $port
      |akka.remote.netty.tcp.tcp-nodelay = on
      """.stripMargin)

      actorSystem = ActorSystem(actorSystemName, akkaConf)
      val provider = actorSystem.asInstanceOf[ExtendedActorSystem].provider
      actorSystemPort = provider.getDefaultAddress.port.get

      // Starting actor
      actorRef = actorSystem.actorOf(actorProps, actorName)

    } catch {
      case e: Exception =>
        println(s"Error starting actor system $actorSystemName and actor $actorName", e)
        if (actorSystem != null) {
          actorSystem.shutdown()
        }
        throw e
    }
    (actorSystem, actorSystemPort, actorRef)
  }

  def waitUntil(condition: () => Boolean, timeoutMillis: Long, timeoutMessage: String = "") {
    val startTimeMillis = System.currentTimeMillis
    while (!condition() && System.currentTimeMillis - startTimeMillis < timeoutMillis) {
      Thread.sleep(100)
    }
    assert(condition(), timeoutMessage)
  }
}
