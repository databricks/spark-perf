import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SparkTestsBuild extends Build {
  lazy val root = Project(
    "spark-perf",
    file("."),
    settings = assemblySettings ++ Seq(
      organization := "org.spark-project",
      version := "0.1",
      scalaVersion := sys.props.getOrElse("scala.version", default="2.11.8"),
      libraryDependencies ++= Seq(
        "net.sf.jopt-simple" % "jopt-simple" % "4.6",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        "com.google.guava" % "guava" % "14.0.1",
        "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
        "org.json4s" %% "json4s-native" % "3.2.9"
      ),
      test in assembly := {},
      outputPath in assembly := file("target/spark-perf-tests-assembly.jar"),
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
