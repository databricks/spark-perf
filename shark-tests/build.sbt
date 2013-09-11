import AssemblyKeys._

name := "shark-perf"

version := "0.1"

organization := "edu.berkeley.cs.amplab"

scalaVersion := "2.9.3"

libraryDependencies += "org.clapper" % "argot_2.9.2" % "0.4"

libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5"

libraryDependencies += "org.scalatest" % "scalatest_2.9.2" % "1.8" % "test"

libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

unmanagedJars in Compile <++= baseDirectory map  { base =>
  val finder: PathFinder = (file("../spark-tests")) ** "*.jar"
  finder.get
}

// Assumes that the 'base' directory is 'perf-tests/shark-tests' and that the Spark, Shark and Hive
// repos are cloned to the 'perf-tests' directory.
unmanagedJars in Compile <++= baseDirectory map  { base =>
  // Assume that 'sbt' is called from 'perf-tests/shark-tests' and that Spark
  // is cloned to perf-tests/spark.
  val finder: PathFinder = (file("../spark")) ** "*.jar"
  finder.get
}

unmanagedJars in Compile <++= baseDirectory map  { base =>
  val hiveFile = file("../hive/build/dist") / "lib"
  val baseDirectories = (base / "lib") +++ (hiveFile)
  val customJars = (baseDirectories ** "*.jar")
  // Hive uses an old version of guava that doesn't have what we want.
  customJars.classpath
    .filter(!_.toString.contains("guava"))
    .filter(!_.toString.contains("log4j"))
    .filter(!_.toString.contains("servlet"))
}

unmanagedJars in Compile <++= baseDirectory map  { base =>
  val finder: PathFinder = (file("../shark")) ** "*.jar"
  finder.get
}

assemblySettings

test in assembly := {}

jarName in assembly := "shark-perf-tests-assembly.jar"

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => 
  {
    case PathList("META-INF", xs @ _*) =>
      (xs.map(_.toLowerCase)) match {
        case ("manifest.mf" :: Nil) => MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") => MergeStrategy.discard
        case _ => MergeStrategy.first
      }
    case PathList("reference.conf", xs @ _*) => MergeStrategy.concat
    case PathList("application.conf", xs @ _*) => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
