import AssemblyKeys._

name := "spark-perf"

version := "0.1"

organization := "org.spark-project"

scalaVersion := "2.9.3"

libraryDependencies += "org.clapper" % "argot_2.9.2" % "0.4"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5"

libraryDependencies += "org.scalatest" % "scalatest_2.9.2" % "1.8" % "test"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

// Assumes that the 'base' directory is 'perf-tests/spark-tests' and that the Spark repo is cloned
// to 'perf-tests/spark'.
unmanagedJars in Compile <++= baseDirectory map  { base =>
  val finder: PathFinder = (file("../spark/assembly/target/scala-2.9.3")) ** "*.jar"
  finder.get
}

assemblySettings

test in assembly := {}

jarName in assembly := "spark-perf-tests-assembly.jar"

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
    case PathList("application.conf", xs @ _*) => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
