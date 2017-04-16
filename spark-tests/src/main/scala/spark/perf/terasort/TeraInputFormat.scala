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

package org.apache.spark.examples.terasort;

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import java.io.EOFException
import java.io.IOException
import java.util.ArrayList
import java.util.List

import com.google.common.primitives.UnsignedBytes
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.IndexedSortable
import org.apache.hadoop.util.QuickSort
import org.apache.hadoop.util.StringUtils

object TeraInputFormat {
   val KEY_LEN = 10
   val VALUE_LEN = 90
   val RECORD_LEN = KEY_LEN + VALUE_LEN
   val PARTITION_FILENAME = "_partition.lst"
   val NUM_PARTITIONS = "mapreduce.terasort.num.partitions"
   val SAMPLE_SIZE = "mapreduce.terasort.partitions.sample"
   var lastContext : JobContext = null
   var lastResult : List[InputSplit] = null
   implicit val caseInsensitiveOrdering = UnsignedBytes.lexicographicalComparator

   class ArraySampler extends IndexedSortable {
     val records = new ArrayList[Array[Byte]]();
     def compare(i : Int, j : Int) : Int = {
       val left : Array[Byte]= records.get(i)
       val right : Array[Byte]= records.get(j)
       caseInsensitiveOrdering.compare(left, right)
     }
     def swap(i : Int, j : Int) = {
         val left : Array[Byte]= records.get(i)
         val right : Array[Byte]= records.get(j)
         records.set(j, left)
         records.set(i, right)
     }
     def addKey(key : Array[Byte]) = {
       this.synchronized {
         records.add(key.clone())
       }
     }

     /**
      * Find the split points for a given sample. The sample keys are sorted
      * and down sampled to find even split points for the partitions. The
      * returned keys should be the start of their respective partitions.
      * @param numPartitions the desired number of partitions
      * @return an array of size numPartitions - 1 that holds the split points
      */
     def createPartitions(numPartitions : Int): Array[Array[Byte]] = {
       val numRecords = records.size();
       println("Making " + numPartitions + " from " + numRecords + " sampled records");
       if (numPartitions > numRecords) {
         throw new IllegalArgumentException
         ("Requested more partitions than input keys (" + numPartitions +
           " > " +  numRecords  +  ")");
       }
       new QuickSort().sort(this, 0, records.size());
       val stepSize : Float = numRecords / numPartitions.toFloat;
       val result : Array[Array[Byte]] = new Array[Array[Byte]](numPartitions-1);
       for(i <- 1 to numPartitions) {
         result.update(i-1, records.get(Math.round(stepSize * i)))
       }
       result
     }
   }
  /**
   * Use the input splits to take samples of the input and generate sample
   * keys. By default reads 100,000 keys from 10 locations in the input, sorts
   * them and picks N-1 keys to generate N equally sized partitions.
   * @param job the job to sample
   * @param partFile where to write the output file to
   * @throws Throwable if something goes wrong
   */
  def writePartitionFile(job : JobContext, partFile : Path) = {
    val t1 = System.currentTimeMillis();
    val conf = job.getConfiguration();
    val inFormat = new TeraInputFormat();
    val sampler = new ArraySampler();
    val partitions = job.getNumReduceTasks();
    val sampleSize = conf.getLong(SAMPLE_SIZE, 100000);
    val splits : List[InputSplit] = inFormat.getSplits(job);
    val t2 = System.currentTimeMillis();
    println("Computing input splits took " + (t2 - t1) + "ms");
    val samples : Int = Math.min(conf.getInt(NUM_PARTITIONS, 10), splits.size());
    println("Sampling " + samples + " splits of " + splits.size());
    val recordsPerSample : Long = sampleSize / samples;
    val sampleStep : Int = splits.size() / samples;
    val samplerReader = new Array[Thread](samples);
    val threadGroup = new SamplerThreadGroup("Sampler Reader Thread Group");
    // take N samples from different parts of the input
    for(i <- 0 to samples) {
      var idx : Int = i
      val thread = new Thread (threadGroup,"Sampler Reader " + idx) {
        setDaemon(true);

        override def run() = {
          var records : Long = 0
          try {
            val context = new TaskAttemptContextImpl(
              job.getConfiguration(), new TaskAttemptID())
            val reader =
              inFormat.createRecordReader(splits.get(sampleStep * idx), context)
            reader.initialize(splits.get(sampleStep * idx), context)
            while (reader.nextKeyValue()) {
              sampler.addKey(reader.getCurrentKey())
              records += 1
              if (recordsPerSample <= records) {
                break
              }
            }
          } catch {
            case e : IOException => {
              println("Got an exception while reading splits " +
                StringUtils.stringifyException(e))
              throw new RuntimeException(e)
            }
            case e: InterruptedException => { }
           }
        } 
      }
      samplerReader.update(i, thread)
    }
    val outFs = partFile.getFileSystem(conf);
    val writer = outFs.create(partFile, true, 64*1024, 10.toShort,
                              outFs.getDefaultBlockSize(partFile))
    for (i <- 0 to samples) {
      try {
        samplerReader(i).join()
        if(threadGroup.getThrowable() != null){
          throw threadGroup.getThrowable()
        }
      } catch { case e : InterruptedException =>  { } }
    }
    for(split <- sampler.createPartitions(partitions)) {
      writer.write(split)
    }
    writer.close()
    val t3 = System.currentTimeMillis()
    println("Computing parititions took " + (t3 - t2) + "ms")
  }

  class SamplerThreadGroup(val s : String) extends ThreadGroup(s) {
    var throwable : Throwable = null

    override def uncaughtException(thread : Thread, throwable : Throwable ) = {
      this.throwable = throwable;
    }
    def getThrowable() : Throwable = {
      this.throwable;
    }
  }
}

class TeraInputFormat extends FileInputFormat[Array[Byte], Array[Byte]] {

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[Array[Byte], Array[Byte]] = new TeraRecordReader()

  override def getSplits(job: JobContext): List[InputSplit] = {
    if (job == TeraInputFormat.lastContext) {
      TeraInputFormat.lastResult;
    }
    TeraInputFormat.lastContext = job
    val t1 = System.currentTimeMillis();
    TeraInputFormat.lastResult = super.getSplits(job);
    val t2 = System.currentTimeMillis();
    println("Spent " + (t2 - t1) + "ms computing base-splits.");

    // NB Java Hadoop Terasort defaults to 'true'.
    if (job.getConfiguration().getBoolean(TeraScheduler.USE, false)) {
      val scheduler = new TeraScheduler(
        TeraInputFormat.lastResult.toArray(new Array[FileSplit](0)), job.getConfiguration())
      TeraInputFormat.lastResult = scheduler.getNewFileSplits()
      val t3 = System.currentTimeMillis()
      println("Spent " + (t3 - t2) + "ms computing TeraSchehduler splits.")
    }
    TeraInputFormat.lastResult
  }

  class TeraRecordReader extends RecordReader[Array[Byte], Array[Byte]] {
    private var in : FSDataInputStream = null
    private var offset: Long = 0
    private var length: Long = 0
    private val buffer: Array[Byte] = new Array[Byte](TeraInputFormat.RECORD_LEN)
    private var key: Array[Byte] = null
    private var value: Array[Byte] = null

    override def nextKeyValue() : Boolean = {
      if (offset >= length) {
        return false
      }
      var read : Int = 0
      while (read < TeraInputFormat.RECORD_LEN) {
        var newRead : Int = in.read(buffer, read, TeraInputFormat.RECORD_LEN - read)
        if (newRead == -1) {
          if (read == 0) false
          else throw new EOFException("read past eof")
        }
        read += newRead
      }
      if (key == null) {
        key = new Array[Byte](TeraInputFormat.KEY_LEN)
      }
      if (value == null) {
        value = new Array[Byte](TeraInputFormat.VALUE_LEN)
      }
      buffer.copyToArray(key, 0, TeraInputFormat.KEY_LEN)
      buffer.takeRight(TeraInputFormat.VALUE_LEN).copyToArray(value, 0, TeraInputFormat.VALUE_LEN)
      offset += TeraInputFormat.RECORD_LEN
      true
    }

    override def initialize(split : InputSplit, context : TaskAttemptContext) = {
      val fileSplit = split.asInstanceOf[FileSplit]
      val p : Path = fileSplit.getPath()
      val fs : FileSystem = p.getFileSystem(context.getConfiguration())
      in = fs.open(p)
      val start : Long = fileSplit.getStart()
      // find the offset to start at a record boundary
      val reclen = TeraInputFormat.RECORD_LEN
      offset = (reclen - (start % reclen)) % reclen
      in.seek(start + offset)
      length = fileSplit.getLength()
    }

    override def close() = in.close()
    override def getCurrentKey() : Array[Byte] = key
    override def getCurrentValue() : Array[Byte] = value
    override def getProgress() : Float = offset / length
  }

}
