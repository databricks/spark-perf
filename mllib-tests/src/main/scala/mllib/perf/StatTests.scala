package mllib.perf

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random.RandomRDDGenerators
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

/** Parent class for the tests for the statistics toolbox
  */
abstract class StatTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RDD[Vector])

  val NUM_ROWS =  ("num-rows",   "number of rows of the matrix")
  val NUM_COLS =  ("num-cols",   "number of columns of the matrix")

  longOptions = Seq(NUM_ROWS)
  intOptions = intOptions ++ Seq(NUM_COLS)

  var rdd: RDD[Vector] = _

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()
  override def createInputData(seed: Long) = {
    val m: Long = longOptionValue(NUM_ROWS)
    val n: Int = intOptionValue(NUM_COLS)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    rdd = RandomRDDGenerators.normalVectorRDD(sc, m, n, numPartitions, seed)
  }

  override def run(): (Double, Double, Double) = {

    val start = System.currentTimeMillis()
    runTest(rdd)
    val end = System.currentTimeMillis()
    val time = (end - start).toDouble / 1000.0

    (time, 0.0, 0.0)
  }

}


class PearsonCorrelationTest(sc: SparkContext) extends StatTests(sc) {
  override def runTest(data: RDD[Vector]) {
     Statistics.corr(rdd)
  }
}

class SpearmanCorrelationTest(sc: SparkContext) extends StatTests(sc) {
  override def runTest(data: RDD[Vector]) {
    Statistics.corr(rdd, "spearman")
  }
}


