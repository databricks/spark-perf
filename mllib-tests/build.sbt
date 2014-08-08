import AssemblyKeys._

name := "mllib-perf"

version := "0.1"

organization := "org.spark-project"

scalaVersion := "2.10.4"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5"

libraryDependencies += "org.scalatest" % "scalatest_2.9.2" % "1.8" % "test"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.2"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.0-SNAPSHOT" % "provided"

assemblySettings

test in assembly := {}

outputPath in assembly := file("target/mllib-perf-tests-assembly.jar")

assemblyOption in assembly ~= { _.copy(includeScala = false) }

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("META-INF", xs @ _*) =>
      (xs.map(_.toLowerCase)) match {
        case ("manifest.mf" :: Nil) => MergeStrategy.discard
        case ("license" :: _) => MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case PathList("reference.conf", xs @ _*) => MergeStrategy.concat
    case PathList("application.conf", xs @ _*) => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
