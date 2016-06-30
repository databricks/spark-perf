package mllib.perf.clustering

import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD

import mllib.perf.PerfTest

class PICTest(sc: SparkContext) extends PerfTest {

  val NUM_EXAMPLES = ("num-examples", "number of examples")
  val NODE_DEGREE = ("node-degree", "number of neighbors each node is connected to")
  val NUM_CENTERS = ("num-centers", "number of centers for clustering tests")
  val NUM_ITERATIONS = ("num-iterations", "number of iterations for the algorithm")

  intOptions ++= Seq(NODE_DEGREE, NUM_CENTERS, NUM_ITERATIONS)
  longOptions ++= Seq(NUM_EXAMPLES)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var data: RDD[(Long, Long, Double)] = _

  override def createInputData(seed: Long): Unit = {
    val numExamples = longOptionValue(NUM_EXAMPLES)
    val nodeDegree = intOptionValue(NODE_DEGREE)
    val numPartitions = intOptionValue(NUM_PARTITIONS)

    // Generates a periodic banded matrix with bandwidth = nodeDegree
    data = sc.parallelize(0L to numExamples, numPartitions)
      .flatMap { id =>
        (((id - nodeDegree / 2) % numExamples) until id).map { nbr =>
          (id, (nbr + numExamples) % numExamples, 1D)
        }
      }
    logInfo(s"Generated ${data.count()} pairwise similarities.")
  }

  override def run(): JValue = {
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val k = intOptionValue(NUM_CENTERS)
    val start = System.currentTimeMillis()
    val pic = new PowerIterationClustering()
      .setK(k)
      .setMaxIterations(numIterations)
    val model = pic.run(data)
    model.assignments.count()
    val duration = (System.currentTimeMillis() - start) / 1e3
    "time" -> duration
  }
}

