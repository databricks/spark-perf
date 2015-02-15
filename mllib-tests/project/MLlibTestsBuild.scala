import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

/**
 * Build settings for MLlib. To build against a specific Spark version (e.g., 1.1.0), use
 * {{{
 * sbt -Dspark.version=1.1.0 clean ...
 * }}}
 */
object MLlibTestsBuild extends Build {

  val sparkVersion = settingKey[String]("Spark version to test against.")

  lazy val commonSettings = Seq(
    organization := "org.spark-project",
    version := "0.1",
    scalaVersion := "2.10.4",
    sparkVersion := sys.props.get("spark.version").getOrElse("1.3.0-SNAPSHOT"),
    libraryDependencies ++= Seq(
      "net.sf.jopt-simple" % "jopt-simple" % "4.6",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.7.2",
      "org.json4s" %% "json4s-native" % "3.2.9",
      "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "provided"
    )
  )

  lazy val root = Project(
    "mllib-perf",
    file("."),
    settings = assemblySettings ++ commonSettings ++ Seq(
      scalaSource in Compile := {
        val targetFolder = sparkVersion.value match {
          case v if v.startsWith("1.0.") => "v1p0"
          case v if v.startsWith("1.1.") => "v1p1"
          case v if v.startsWith("1.2.") => "v1p2"
          case v if v.startsWith("1.3.") => "v1p3"
          case _ => throw new IllegalArgumentException(s"Do not support Spark $sparkVersion.")
        }
        baseDirectory.value / targetFolder / "src"
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
