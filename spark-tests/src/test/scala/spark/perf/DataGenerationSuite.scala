package spark.perf

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.matchers.ShouldMatchers._
import org.scalatest.matchers.ShouldMatchers
import com.google.common.io.Files
import org.apache.spark.SparkContext
import com.google.common.hash.Hashing

class DataGenerationSuite extends FunSuite with BeforeAndAfterAll with ShouldMatchers {
  private var _sc: SparkContext = _
  private var _workingDir: String = _

  def sc: SparkContext = _sc
  def workingDir: String = _workingDir

  override def beforeAll() {
    _sc = new SparkContext("local", "test")
    _workingDir = Files.createTempDir().getAbsolutePath
    super.beforeAll()
  }

  override def afterAll() {
    if (_sc != null) {
      _sc.stop()
      _sc = null
      _workingDir = null
    }
  }

  test("creates correct number of records") {
    val data = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 3333,
      persistenceType = "memory")

    data.count should equal (1000)
  }

  test("creates correct key and value length") {
    val data = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 3333,
      persistenceType = "memory")
    val records = data.collect()

    val maxKeyLength = records.map{case (k, v) => k.length}.max
    val minKeyLength = records.map{case (k, v) => k.length}.min
    val maxValueLength = records.map{case (k, v) => v.length}.max
    val minValueLength = records.map{case (k, v) => v.length}.min

    maxKeyLength should be (30)
    minKeyLength should be (30)
    maxValueLength should be (30)
    minValueLength should be (30)
  }

  test("partitions use different random generators") {
    val data = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 3333,
      persistenceType = "memory")

    val partitionData = data.glom.collect.zipWithIndex
    partitionData.size should be (5)
    for (a <- partitionData; b <- partitionData) {
      if (a._2 == b._2) {
        a._1 should equal(b._1)
      } else {
        a._1 should not equal (b._1)
      }
    }
  }

  test("datasets are identical for disk, memory, and HDFS") {
    val memData = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 5555,
      persistenceType = "memory").collect()

    val diskData = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 5555,
      persistenceType = "disk").collect()

    val hdfsData = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 5555,
      persistenceType = "hdfs",
      storageLocation = workingDir + "/hdfsData").collect()

    // These are entirely subsumed by the final test, but remain useful for debugging failures:
    memData.size should equal (diskData.size)
    memData.size should equal (hdfsData.size)

    val memUniqueKeys = memData.map(_._1).distinct.toSet
    val diskUniqueKeys = diskData.map(_._1).distinct.toSet
    val hdfsUniqueKeys = hdfsData.map(_._1).distinct.toSet

    memUniqueKeys.size should equal (diskUniqueKeys.size)
    memUniqueKeys.size should equal (hdfsUniqueKeys.size)
    memUniqueKeys should equal(diskUniqueKeys)
    memUniqueKeys should equal (hdfsUniqueKeys)

    val memUniqueValues = memData.map(_._2).distinct.toSet
    val diskUniqueValues = diskData.map(_._2).distinct.toSet
    val hdfsUniqueValues = hdfsData.map(_._2).distinct.toSet

    memUniqueValues.size should equal (diskUniqueValues.size)
    memUniqueValues.size should equal (hdfsUniqueValues.size)
    memUniqueValues should equal(diskUniqueValues)
    memUniqueValues should equal (hdfsUniqueValues)

    // All encompassing equality tests
    memData should equal (diskData)
    // NOTE: For HDFS here we allow the RDD to have a different ordering. This is necessary because
    // the read might chose a different partitioning scheme.
    memData.sorted should equal (hdfsData.sorted)
  }

  test("random seeds produce different but consistent results") {
    val data1 = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 3333,
      persistenceType = "memory")

    val data2 = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 3333,
      persistenceType = "memory")

    data1.collect() should equal (data2.collect())

    val data3 = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 4444,
      persistenceType = "memory")

    val data4 = DataGenerator.createKVStringDataSet(sc,
      numRecords = 1000,
      uniqueKeys = 10,
      keyLength = 30,
      uniqueValues = 100,
      valueLength = 30,
      numPartitions = 5,
      randomSeed = 4444,
      persistenceType = "memory")

    data3.collect() should equal (data4.collect())

    data1.collect() should not equal (data3.collect())
  }

  test("cardinality of keys and values are as expected") {
    def run(_uniqueKeys: Int, _uniqueValues: Int) = {
      val data = DataGenerator.createKVStringDataSet(sc,
        numRecords = 10000,
        uniqueKeys = _uniqueKeys,
        keyLength = 30,
        uniqueValues = _uniqueValues,
        valueLength = 30,
        numPartitions = 10,
        randomSeed = 3333,
        persistenceType = "memory").collect()

      val records: Seq[(Int, Int)] = data.map{case (k, v) => (k.toInt, v.toInt)}

      val uniqueKeys = records.map(_._1).distinct
      val uniqueValues = records.map(_._2).distinct
      val uniquePairs = records.distinct

      uniqueKeys.toSet should equal ((0 to _uniqueKeys - 1).toSet)
      uniqueValues.toSet should equal ((0 to _uniqueValues - 1).toSet)
      uniquePairs.size should equal (_uniqueKeys * _uniqueValues)
    }
    run(5, 5)
    run(2, 10)
    run(10, 2)
  }


  test("cardinality of keys and values are as expected (hashing)") {
    def run(_uniqueKeys: Int, _uniqueValues: Int) = {
      val data: Seq[(String, String)] = DataGenerator.createKVStringDataSet(sc,
        numRecords = 10000,
        uniqueKeys = _uniqueKeys,
        keyLength = 30,
        uniqueValues = _uniqueValues,
        valueLength = 30,
        numPartitions = 10,
        randomSeed = 3333,
        persistenceType = "memory",
        hashFunction = Some(Hashing.goodFastHash(30 * 4))).collect()

      val uniqueKeys = data.map(_._1).distinct
      val uniqueValues = data.map(_._2).distinct
      val uniquePairs = data.distinct

      uniqueKeys.size should equal (_uniqueKeys)
      uniqueValues.size should equal (_uniqueValues)
      uniquePairs.size should equal (_uniqueKeys * _uniqueValues)
    }
    run(5, 5)
    run(2, 10)
    run(10, 2)
  }

  test("integer datasets") {
    def run(_uniqueKeys: Int, _uniqueValues: Int) = {
      val records: Seq[(Int, Int)] = DataGenerator.createKVIntDataSet(sc,
        numRecords = 10000,
        uniqueKeys = _uniqueKeys,
        uniqueValues = _uniqueValues,
        numPartitions = 10,
        randomSeed = 3333,
        persistenceType = "memory").collect()

      records.length should equal (10000)

      val uniqueKeys = records.map(_._1).distinct
      val uniqueValues = records.map(_._2).distinct
      val uniquePairs = records.distinct

      uniqueKeys.toSet should equal ((0 to _uniqueKeys - 1).toSet)
      uniqueValues.toSet should equal ((0 to _uniqueValues - 1).toSet)
      uniquePairs.size should equal (_uniqueKeys * _uniqueValues)
    }
    run(5, 5)
    run(2, 10)
    run(10, 2)
  }
}
