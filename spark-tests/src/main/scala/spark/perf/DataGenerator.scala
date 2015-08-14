package spark.perf

import java.util.Random

import com.google.common.hash.HashFunction
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object DataGenerator {

  /** Encode the provided integer as a fixed-length string. If a hash function is provided,
    * the integer is hashed then encoded. */
  def paddedString(i: Int, length: Int, hashFunction: Option[HashFunction] = None): String = {
    hashFunction match {
      case Some(hash) =>
        val out = hash.hashInt(i).toString.take(length)
        require(out.length == length, s"Hash code was too short for requested length: $length")
        out
      case None =>
        val fmtString = "%%0%sd".format(length)
        fmtString.format(i)
    }
  }

  /** Creates a key-value int dataset but does not cache it, allowing for subsequent processing */
  private def generateIntData(
      sc: SparkContext,
      numRecords: Long,
      uniqueKeys: Int,
      uniqueValues: Int,
      numPartitions: Int,
      randomSeed: Int)
    : RDD[(Int, Int)] =
  {
    val recordsPerPartition = (numRecords / numPartitions.toDouble).toInt

    def generatePartition(index: Int) = {
      // Use per-partition seeds to avoid having identical data at all partitions
      val effectiveSeed = (randomSeed ^ index).toString.hashCode
      val r = new Random(effectiveSeed)
      (1 to recordsPerPartition).map{i =>
        val key = r.nextInt(uniqueKeys)
        val value = r.nextInt(uniqueValues)
        (key, value)
      }.iterator
    }

    sc.parallelize(Seq[Int](), numPartitions).mapPartitionsWithIndex{case (index, n) => {
      generatePartition(index)
    }}
  }

  /** Creates and materializes a (K, V) int dataset according to the supplied parameters. */
  def createKVIntDataSet(
      sc: SparkContext,
      numRecords: Long,
      uniqueKeys: Int,
      uniqueValues: Int,
      numPartitions: Int,
      randomSeed: Int,
      persistenceType: String,
      storageLocation: String = "/tmp/spark-perf-kv-data")
    : RDD[(Int, Int)] =
  {
    val inputRDD = generateIntData(
      sc, numRecords, uniqueKeys, uniqueValues, numPartitions, randomSeed)

    val rdd = persistenceType match {
      case "memory" => {
        val tmp = inputRDD.persist(StorageLevel.MEMORY_ONLY)
        tmp.count()
        tmp
      }
      case "disk" => {
        val tmp = inputRDD.persist(StorageLevel.DISK_ONLY)
        tmp.count()
        tmp
      }
      case "hdfs" => {
        val storagePath = new Path(storageLocation)
        val fileSystem = storagePath.getFileSystem(new Configuration())
        if (!fileSystem.exists(storagePath)) {
          inputRDD.map{case (k, v) => "%s\t%s".format(k, v)}
                  .saveAsTextFile(storageLocation, classOf[DefaultCodec])
        } else {
          println(s"ATTENTION: Using input data already stored in $storageLocation. " +
            s"It is not guaranteed to be consistent with provided parameters.")
        }
        sc.textFile(storageLocation).map(_.split("\t")).map(x => (x(0).toInt, x(1).toInt))
      }
      case unknown => {
        throw new Exception(s"Unrecognized persistence option: $unknown")
      }
    }
    rdd
  }

  /** Creates and materializes a (K, V) string dataset according to the supplied parameters. */
  def createKVStringDataSet(
      sc: SparkContext,
      numRecords: Long,
      uniqueKeys: Int,
      keyLength: Int,
      uniqueValues: Int,
      valueLength: Int,
      numPartitions: Int,
      randomSeed: Int,
      persistenceType: String,
      storageLocation: String = "/tmp/spark-perf-kv-data",
      hashFunction: Option[HashFunction] = None)
    : RDD[(String, String)] =
  {
    val ints = generateIntData(
      sc, numRecords, uniqueKeys, uniqueValues, numPartitions, randomSeed)
    val inputRDD = ints.map { case (k, v) =>
      (paddedString(k, keyLength, hashFunction), paddedString(v, valueLength, hashFunction))
    }

    val rdd = persistenceType match {
      case "memory" => {
        val tmp = inputRDD.persist(StorageLevel.MEMORY_ONLY)
        tmp.count()
        tmp
      }
      case "disk" => {
        val tmp = inputRDD.persist(StorageLevel.DISK_ONLY)
        tmp.count()
        tmp
      }
      case "hdfs" => {
        val storagePath = new Path(storageLocation)
        val fileSystem = storagePath.getFileSystem(new Configuration())
        val pathExists = fileSystem.exists(storagePath) && fileSystem.listStatus(storagePath).length > 0
        if (!pathExists) {
          inputRDD.map{case (k, v) => "%s\t%s".format(k, v)}
            .saveAsTextFile(storageLocation, classOf[DefaultCodec])
        } else {
          println(s"ATTENTION: Using input data already stored in $storageLocation. " +
            s"It is not guaranteed to be consistent with provided parameters.")
        }
        sc.textFile(storageLocation).map(_.split("\t")).map(x => (x(0), x(1)))
      }
      case unknown => {
        throw new Exception(s"Unrecognized persistence option: $unknown")
      }
    }
    rdd
  }
}
