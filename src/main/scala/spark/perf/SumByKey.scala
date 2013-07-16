package spark.perf

import spark.{RDD, SparkContext}
import spark.SparkContext._

class SumByKey(sc: SparkContext) extends KVDataTest(sc) {
  override def getName = "Sum By Key"
  override def getVersion = 1
  override def runTest(rdd: RDD[(String, String)], reduceTasks: Int) {
    rdd.map{case (k, v) => (k, v.toInt)}.reduceByKey(_ + _, reduceTasks)
  }
}
