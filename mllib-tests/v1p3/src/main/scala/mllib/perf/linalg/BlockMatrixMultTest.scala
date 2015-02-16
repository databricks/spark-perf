package mllib.perf.linalg

import java.util.Random

import org.json4s.JValue
import org.json4s.JsonDSL._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

import mllib.perf.PerfTest

class BlockMatrixMultTest(sc: SparkContext) extends PerfTest {

  val M = ("m", "number of rows of A")
  val K = ("k", "number of columns of A, the same as number of rows of B")
  val N = ("n", "number of columns of B")
  val BLOCK_SIZE = ("block-size", "block size")

  intOptions ++= Seq(BLOCK_SIZE)
  longOptions ++= Seq(M, K, N)

  val options = intOptions ++ stringOptions  ++ booleanOptions ++ doubleOptions ++ longOptions
  addOptionsToParser()

  private var A: BlockMatrix = _
  private var B: BlockMatrix = _

  override def createInputData(seed: Long): Unit = {
    val m = longOptionValue(M)
    val k = longOptionValue(K)
    val n = longOptionValue(N)
    val blockSize = intOptionValue(BLOCK_SIZE)
    val numPartitions = intOptionValue(NUM_PARTITIONS)

    val random = new Random(seed)

    A = randn(m, k, blockSize, numPartitions, seed ^ random.nextLong())
    B = randn(k, n, blockSize, numPartitions, seed ^ random.nextLong())
  }

  def randn(
      m: Long,
      n: Long,
      blockSize: Int,
      numPartitions: Int,
      seed: Long): BlockMatrix = {
    val numRowBlocks = math.ceil(m / blockSize).toInt
    val numColBlocks = math.ceil(n / blockSize).toInt
    val sqrtParts = math.ceil(math.sqrt(numPartitions)).toInt
    val rowBlockIds = sc.parallelize(0 until numRowBlocks, sqrtParts)
    val colBlockIds = sc.parallelize(0 until numColBlocks, sqrtParts)
    val blockIds = rowBlockIds.cartesian(colBlockIds)
    val blocks = blockIds.mapPartitionsWithIndex { (idx, ids) =>
      val random = new Random(idx ^ seed)
      ids.map { case (rowBlockId, colBlockId) =>
        val mi = math.min(m - rowBlockId * blockSize, blockSize).toInt
        val ni = math.min(n - colBlockId * blockSize, blockSize).toInt
        ((rowBlockId, colBlockId), Matrices.randn(mi, ni, random))
      }
    }.cache()
    logInfo(s"Generated ${blocks.count()} blocks.")
    new BlockMatrix(blocks, blockSize, blockSize, m, n)
  }

  override def run(): JValue = {
    val start = System.currentTimeMillis()
    val C = A.multiply(B)
    C.blocks.count()
    val duration = (System.currentTimeMillis() - start) / 1e3
    "time" -> duration
  }
}
