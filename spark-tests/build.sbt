import AssemblyKeys._

name := "spark-perf"

version := "0.1"

organization := "org.spark-project"

scalaVersion := "2.10.3"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0" % "provided"

assemblySettings

test in assembly := {}

outputPath in assembly := file("target/spark-perf-tests-assembly.jar")

assemblyOption in assembly ~= { _.copy(includeScala = false) }

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", xs @ _*) =>
      (xs.map(_.toLowerCase)) match {
        case ("manifest.mf" :: Nil) => MergeStrategy.discard
        // Note(harvey): this to get Shark perf test assembly working.
        case ("license" :: _) => MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case PathList("reference.conf", xs @ _*) => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.discard
    case PathList("application.conf", xs @ _*) => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
