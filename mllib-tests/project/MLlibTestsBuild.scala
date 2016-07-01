import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

/**
 * Build settings for MLlib. To build against a specific Spark version (e.g., 1.1.0), use
 * {{{
 * sbt -Dspark.version=1.4.1 clean ...
 * }}}
 */
object MLlibTestsBuild extends Build {

  val sparkVersion = settingKey[String]("Spark version to test against.")

  lazy val commonSettings = Seq(
    organization := "org.spark-project",
    version := "0.1",
    scalaVersion := sys.props.getOrElse("scala.version", default="2.11.8"),
    sparkVersion := sys.props.getOrElse("spark.version", default="2.0.0"),
    libraryDependencies ++= Seq(
      "net.sf.jopt-simple" % "jopt-simple" % "4.6",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.7.2",
      "org.json4s" %% "json4s-native" % "3.2.9"

      // IMPORTANT!
      // We need to uncomment the below once Spark 2.0.0 becomes available
      // This relies on using spark built under the lib folder 
      // of this project
      
      //"org.apache.spark" %% "spark-core" % "2.0.0-SNAPSHOT" % "provided",
      //"org.apache.spark" %% "spark-mllib" % "2.0.0-SNAPSHOT" % "provided"
    )
  )

  lazy val root = Project(
    "mllib-perf",
    file("."),
    settings = assemblySettings ++ commonSettings ++ Seq(
      scalaSource in Compile := {
        println("sparkVersion.value is: " + sparkVersion.value)
        val targetFolder = sparkVersion.value match {
          case v if v.startsWith("1.4.") => "v1p4"
          case v if v.startsWith("1.5.") => "v1p5" // acceptable for now, but change later when new algs are added
          case v if v.startsWith("1.6.") => "v1p5"
          case v if v.startsWith("2.0") => "v2p0"
          case _ => throw new IllegalArgumentException(s"This Spark version isn't supported: ${sparkVersion.value}.")
        }
        baseDirectory.value / targetFolder / "src" / "main" / "scala"
      },
      test in assembly := {},
      outputPath in assembly := file("target/mllib-perf-tests-assembly.jar"),
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
