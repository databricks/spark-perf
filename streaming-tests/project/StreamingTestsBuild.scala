import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object StreamingTestsBuild extends Build {
  lazy val root = Project(
    "streaming-perf",
    file("."),
    settings = assemblySettings ++ Seq(
      organization := "org.spark-project",
      version := "0.1",
      scalaVersion := sys.props.getOrElse("scala.version", default="2.11.8"),
      libraryDependencies ++= Seq(
        "net.sf.jopt-simple" % "jopt-simple" % "4.5",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        "com.google.guava" % "guava" % "14.0.1",
        "com.typesafe.akka" %% "akka-actor"   % "2.3.11",
        "com.typesafe.akka" %% "akka-slf4j"   % "2.3.11",
        "com.typesafe.akka" %% "akka-remote"  % "2.3.11",
        "com.typesafe.akka" %% "akka-agent"   % "2.3.11",
        "org.slf4j" % "slf4j-log4j12" % "1.7.2",
        "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
        "org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided"
      ),
      test in assembly := {},
      outputPath in assembly := file("target/streaming-perf-tests-assembly.jar"),
      assemblyOption in assembly ~= { _.copy(includeScala = false) },
      mergeStrategy in assembly := {
        case PathList("META-INF", xs@_*) =>
          (xs.map(_.toLowerCase)) match {
            case ("manifest.mf" :: Nil) => MergeStrategy.discard
            // Note(harvey): this to get Shark perf test assembly working.
            case ("license" :: _) => MergeStrategy.discard
            case ps@(x :: xs) if ps.last.endsWith(".sf") => MergeStrategy.discard
            case _ => MergeStrategy.first
          }
        case PathList("reference.conf", xs@_*) => MergeStrategy.concat
        case "log4j.properties" => MergeStrategy.discard
        case PathList("application.conf", xs@_*) => MergeStrategy.concat
        case _ => MergeStrategy.first
      }
    ))
}
