package mllib.perf.clustering

import java.util.Random

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.GaussianMixture
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import mllib.perf.PerfTest

class GaussianMixtureTest(sc: SparkContext) extends PerfTest {

  val NUM_POINTS = ("num-points", "number of points for clustering tests")
  val NUM_COLUMNS = ("num-columns", "number of columns for each point for clustering tests")
  val NUM_CENTERS = ("num-centers", "number of centers for clustering tests")
  val NUM_ITERATIONS = ("num-iterations", "number of iterations for the algorithm")

  intOptions ++= Seq(NUM_CENTERS, NUM_COLUMNS, NUM_ITERATIONS)
  longOptions ++= Seq(NUM_POINTS)
  val options = intOptions ++ stringOptions  ++ booleanOptions ++ longOptions ++ doubleOptions
  addOptionsToParser()

  var data: RDD[Vector] = _

  override def createInputData(seed: Long): Unit = {
    val m = longOptionValue(NUM_POINTS)
    val n = intOptionValue(NUM_COLUMNS)
    val k = intOptionValue(NUM_CENTERS)
    val p = intOptionValue(NUM_PARTITIONS)

    val random = new Random(seed ^ 8793480384L)
    val mu = Array.fill(k)(new BDV[Double](Array.fill(n)(random.nextGaussian())))
    val f = Array.fill(k)(new BDM[Double](n, n, Array.fill(n * n)(random.nextGaussian())))
    data = sc.parallelize(0L until m, p)
      .mapPartitionsWithIndex { (idx, part) =>
        val rng = new Random(seed & idx)
        part.map { _ =>
          val i = (rng.nextDouble() * k).toInt
          val x = new BDV[Double](Array.fill(n)(rng.nextGaussian()))
          val y = f(i) * x + mu(i)
          Vectors.dense(y.data)
        }
      }.cache()
    logInfo(s"Generated ${data.count()} points.")
  }

  override def run(): JValue = {
    val numIterations = intOptionValue(NUM_ITERATIONS)
    val k = intOptionValue(NUM_CENTERS)
    val start = System.currentTimeMillis()
    val gmm = new GaussianMixture()
      .setK(k)
      .setMaxIterations(numIterations)
    val model = gmm.run(data)
    val duration = (System.currentTimeMillis() - start) / 1e3
    "time" -> duration
  }
}
