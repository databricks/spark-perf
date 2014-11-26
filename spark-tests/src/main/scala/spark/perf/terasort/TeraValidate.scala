/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.terasort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.rdd.RDD
import org.apache.hadoop.util.PureJavaCrc32
import com.google.common.primitives.UnsignedBytes

/**
 * An application that reads sorted data according to the terasort spec and
 * reports if it's indeed sorted.
 * This is an example program to validate TeraSort results
 *
 * See http://sortbenchmark.org/
 */

object TeraValidate {

  def main(args: Array[String]) {

    if (args.length < 1) {
      println("usage:")
      println("DRIVER_MEMORY=[mem] bin/run-example " +
        "org.apache.spark.examples.terasort.TeraValidate " +
        "[input-directory]")
      println(" ")
      println("example:")
      println("DRIVER_MEMORY=50g bin/run-example " +
        "org.apache.spark.examples.terasort.TeraValidate " +
        "file:///scratch/username/terasort_in ")
      System.exit(0)
    }

    // Process command line arguments
    val inputFile = args(0)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"TeraValidate")
    val sc = new SparkContext(conf)

    validate(sc, inputFile )
  }

  def validate(sc : SparkContext, inputFile: String) : Unit = {
    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)

    val output : RDD[(Unsigned16, Array[Byte], Array[Byte])] = 
      dataset.map{case (k,v) => (k.clone(), v.clone())}.mapPartitions( (iter) => {

        val sum = new Unsigned16
        val checksum = new Unsigned16
        val crc32 = new PureJavaCrc32()
        val min = new Array[Byte](10)
        val max = new Array[Byte](10)

        val cmp = UnsignedBytes.lexicographicalComparator()

        var pos = 0L
        var prev = new Array[Byte](10)

        while (iter.hasNext) {
          val key = iter.next()._1
          assert(cmp.compare(key, prev) >= 0)

          crc32.reset()
          crc32.update(key, 0, key.length)
          checksum.set(crc32.getValue)
          sum.add(checksum)

          if (pos == 0) {
            key.copyToArray(min, 0, 10)
          } 
          pos += 1
          prev = key
        }
        prev.copyToArray(max, 0, 10)
        Iterator((sum, min, max))
      }, true)

      val checksumOutput = output.collect()
      val cmp = UnsignedBytes.lexicographicalComparator()
      val sum = new Unsigned16
      var numRecords = dataset.count

      checksumOutput.foreach { case (partSum, min, max) =>
        sum.add(partSum)
      }
      println("num records: " + numRecords)
      println("checksum: " + sum.toString)
      var lastMax = new Array[Byte](10)
      checksumOutput.map{ case (partSum, min, max) =>
        (partSum, min.clone(), max.clone())
      }.zipWithIndex.foreach { case ((partSum, min, max), i) =>
        println(s"part $i")
        println(s"lastMax" + lastMax.toSeq.map(x => if (x < 0) 256 + x else x))
        println(s"min " + min.toSeq.map(x => if (x < 0) 256 + x else x))
        println(s"max " + max.toSeq.map(x => if (x < 0) 256 + x else x))
        assert(cmp.compare(min, max) <= 0, "min >= max")
        assert(cmp.compare(lastMax, min) <= 0, "current partition min < last partition max")
        lastMax = max
      }
      println("num records: " + numRecords)
      println("checksum: " + sum.toString)
      println("partitions are properly sorted")
  }
}
