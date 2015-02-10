package mllib.perf

import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import mllib.perf.util.DataGenerator

/** Parent class for linear algebra tests which run on a large dataset.
  * Generated this way so that SVD / PCA can be added easily
  */
abstract class LinearAlgebraTests(sc: SparkContext) extends PerfTest {

  def runTest(rdd: RowMatrix, rank: Int)

  val NUM_ROWS =  ("num-rows",   "number of rows of the matrix")
  val NUM_COLS =  ("num-cols",   "number of columns of the matrix")
  val RANK =  ("rank",   "number of leading singular values")

  longOptions = Seq(NUM_ROWS)
  intOptions = intOptions ++ Seq(RANK, NUM_COLS)

  var rdd: RowMatrix = _

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()
  override def createInputData(seed: Long) = {
    val m: Long = longOptionValue(NUM_ROWS)
    val n: Int = intOptionValue(NUM_COLS)
    val numPartitions: Int = intOptionValue(NUM_PARTITIONS)

    rdd = DataGenerator.generateDistributedSquareMatrix(sc, m, n, numPartitions, seed)
  }

  override def run(): JValue = {
    val rank = intOptionValue(RANK)

    val start = System.currentTimeMillis()
    runTest(rdd, rank)
    val end = System.currentTimeMillis()
    val time = (end - start).toDouble / 1000.0

    Map("time" -> time)
  }
}


class SVDTest(sc: SparkContext) extends LinearAlgebraTests(sc) {
  override def runTest(data: RowMatrix, rank: Int) {
     data.computeSVD(rank, computeU = true)
  }
}

class PCATest(sc: SparkContext) extends LinearAlgebraTests(sc) {
  override def runTest(data: RowMatrix, rank: Int) {
    val principal = data.computePrincipalComponents(rank)
    sc.broadcast(principal)
    data.multiply(principal)
  }
}

class ColumnSummaryStatisticsTest(sc: SparkContext) extends LinearAlgebraTests(sc) {
  override def runTest(data: RowMatrix, rank: Int) {
    data.computeColumnSummaryStatistics()
  }
}
