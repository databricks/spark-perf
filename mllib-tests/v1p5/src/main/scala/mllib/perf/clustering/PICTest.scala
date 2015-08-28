package mllib.perf.clustering

import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.mllib.clustering.PowerIterationClustering

import mllib.perf.PerfTest

class PICTest(sc: SparkContext) extends PerfTest {

  val NUM_POINTS_SQUARED = ("num-points-squared", "number of points squared (for linear test-size scaling)")
  val NODE_DEGREE = ("node-degree", "number of neighbors each node is connected to")
  val NUM_CENTERS = ("num-centers", "number of centers for clustering tests")
  val NUM_ITERATIONS = ("num-iterations", "number of iterations for the algorithm")

  intOptions ++= Seq(NODE_DEGREE, NUM_CENTERS, NUM_ITERATIONS)
  longOptions ++= Seq(NUM_POINTS_SQUARED)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var data: Graph[Double, Double] = _

  override def createInputData(seed: Long): Unit = {
    val numPoints = math.sqrt(longOptionValue(NUM_POINTS_SQUARED)).toInt
    val nodeDegree = intOptionValue(NODE_DEGREE)
    val numCenters = intOptionValue(NUM_CENTERS)
    val numPartitions = intOptionValue(NUM_PARTITIONS)

    // Generates a periodic banded matrix with bandwidth nodeDegree / 2
    val similarities = sc.parallelize(0L to numPoints)
      .flatMap { id =>
        (((id - nodeDegree / 2) % numPoints) to ((id + nodeDegree / 2) % numPoints)).map { nbr =>
          (id, nbr, 1D)
        }
      }
    val edges = similarities.flatMap { case (i, j, s) =>
      if (i != j) {
        Seq(Edge(i, j, s), Edge(j, i, s))
      } else {
        None
      }
    }
    data = Graph.fromEdges(edges, 0.0)
    logInfo(s"Generated ${similarities.count()} pairwise similarities.")
  }

  override def run(): JValue = {
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val k = intOptionValue(NUM_CENTERS)
    val start = System.currentTimeMillis()
    val pic = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(numIterations)
    val model = pic.run(data)
    val duration = (System.currentTimeMillis() - start) / 1e3
    "time" -> duration
  }
}

