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

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object TeraGen {
  def main(args: Array[String]) {

    if (args.length < 2) {
      println("usage:")
      println("DRIVER_MEMORY=[mem] bin/run-example terasort.TeraGen " +
        "[output-size] [output-directory]")
      println(" ")
      println("example:")
      println("DRIVER_MEMORY=50g bin/run-example terasort.TeraGen " +
        "100G file:///scratch/username/terasort_in")
      System.exit(0)
    }

    // Process command line arguments
    val outputSizeInBytes = sizeStrToBytes(args(0))
    val outputFile = args(1)

    val size = sizeToSizeStr(outputSizeInBytes)

    val conf = new SparkConf()
      .setAppName(s"TeraSort ($size)")
    val sc = new SparkContext(conf)

    val parts = sc.defaultMinPartitions
    val recordsPerPartition = outputSizeInBytes / 100 / parts.toLong
    val numRecords = recordsPerPartition * parts.toLong

    println("===========================================================================")
    println("===========================================================================")
    println(s"Input size: $size")
    println(s"Total number of records: $numRecords")
    println(s"Number of output partitions: $parts")
    println("Number of records/output partition: " + (numRecords / parts))
    println("===========================================================================")
    println("===========================================================================")

    assert(recordsPerPartition < Int.MaxValue, s"records per partition > ${Int.MaxValue}")


    val dataset = sc.parallelize(1 to parts, parts).mapPartitionsWithIndex { case (index, _) =>
      val one = new Unsigned16(1)
      val firstRecordNumber = new Unsigned16(index.toLong * recordsPerPartition.toLong)
      val recordsToGenerate = new Unsigned16(recordsPerPartition)

      val recordNumber = new Unsigned16(firstRecordNumber)
      val lastRecordNumber = new Unsigned16(firstRecordNumber)
      lastRecordNumber.add(recordsToGenerate)

      val rand = Random16.skipAhead(firstRecordNumber)

      val rowBytes: Array[Byte] = new Array[Byte](TeraInputFormat.RECORD_LEN)
      val key = new Array[Byte](TeraInputFormat.KEY_LEN)
      val value = new Array[Byte](TeraInputFormat.VALUE_LEN)

      Iterator.tabulate(recordsPerPartition.toInt) { offset =>
        Random16.nextRand(rand)
        generateRecord(rowBytes, rand, recordNumber)
        recordNumber.add(one)
        rowBytes.copyToArray(key, 0, TeraInputFormat.KEY_LEN)
        rowBytes.copyToArray(value, TeraInputFormat.KEY_LEN, TeraInputFormat.VALUE_LEN)
        (key, value)
      }
    }

    dataset.saveAsNewAPIHadoopFile[TeraOutputFormat](outputFile)

    println("Number of records written: " + dataset.count())
  }

  def sizeStrToBytes(str: String): Long = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      lower.substring(0, lower.length - 1).toLong * 1000
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length - 1).toLong * 1000 * 1000 * 1000 * 1000
    } else {
      // no suffix, so it's just a number in bytes
      lower.toLong
    }
  }

  def sizeToSizeStr(size: Long): String = {
    val kbScale: Long = 1000
    val mbScale: Long = 1000 * kbScale
    val gbScale: Long = 1000 * mbScale
    val tbScale: Long = 1000 * gbScale
    if (size > tbScale) {
      size / tbScale + "TB"
    } else if (size > gbScale) {
      size / gbScale  + "GB"
    } else if (size > mbScale) {
      size / mbScale + "MB"
    } else if (size > kbScale) {
      size / kbScale + "KB"
    } else { 
      size + "B"
    }
  }

  /**
   * Generate a binary record suitable for all sort benchmarks except PennySort.
   *
   * @param recBuf record to return
   */
  def generateRecord(recBuf: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      recBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    recBuf(10) = 0x00.toByte
    recBuf(11) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record.
    i = 0
    while (i < 32) {
      recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(44) = 0x88.toByte
    recBuf(45) = 0x99.toByte
    recBuf(46) = 0xAA.toByte
    recBuf(47) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      recBuf(48 + i * 4) = v
      recBuf(49 + i * 4) = v
      recBuf(50 + i * 4) = v
      recBuf(51 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(96) = 0xCC.toByte
    recBuf(97) = 0xDD.toByte
    recBuf(98) = 0xEE.toByte
    recBuf(99) = 0xFF.toByte
  }

}
