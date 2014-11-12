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

import java.util.List;
import java.io.EOFException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class TeraRecordReader extends RecordReader[Array[Byte], Array[Byte]] {
  var in : FSDataInputStream = null
  var offset: Long = 0
  var length: Long = 0
  val buffer: Array[Byte] = new Array[Byte](RecordWrapper.RECORD_LEN)
  var key: Array[Byte] = null
  var value: Array[Byte] = null

  override def nextKeyValue() : Boolean = {
      if (offset >= length) {
        false
      }
      var read : Int = 0
      while (read < RecordWrapper.RECORD_LEN) {
        var newRead : Int = in.read(buffer, read, RecordWrapper.RECORD_LEN - read)
        if (newRead == -1) {
          if (read == 0) {
            return false;
          } else {
            throw new EOFException("read past eof");
          }
        }
        read += newRead
      }
      if (key == null) {
        key = new Array[Byte](RecordWrapper.KEY_LEN)
      }
      if (value == null) {
        value = new Array[Byte](RecordWrapper.VALUE_LEN)
      }
      buffer.copyToArray(key, 0, RecordWrapper.KEY_LEN)
      buffer.copyToArray(value, RecordWrapper.KEY_LEN, RecordWrapper.VALUE_LEN)
      offset += RecordWrapper.RECORD_LEN
      true
    }

  override def initialize(split : InputSplit, context : TaskAttemptContext) = {
    val fileSplit = split.asInstanceOf[FileSplit]
    val p : Path = fileSplit.getPath()
    val fs : FileSystem = p.getFileSystem(context.getConfiguration())
    in = fs.open(p)
    val start : Long = fileSplit.getStart()
    // find the offset to start at a record
    // boundary
    val reclen = RecordWrapper.RECORD_LEN
    offset = (reclen - (start % reclen)) % reclen
    in.seek(start + offset)
    length = fileSplit.getLength()
  }

  override def close() = in.close()
  override def getCurrentKey() : Array[Byte] = key
  override def getCurrentValue() : Array[Byte] = value
  override def getProgress() : Float = offset / length
}

class TeraInputFormat extends FileInputFormat[Array[Byte], Array[Byte]] {
  var lastContext : JobContext = null
  var lastResult : List[InputSplit] = null
  override def createRecordReader(split: InputSplit, 
    context: TaskAttemptContext): RecordReader[Array[Byte], Array[Byte]] = new TeraRecordReader()

  override def getSplits(job: JobContext): List[InputSplit] = {
    if (job == lastContext) {
      return lastResult;
    }
    lastContext = job
    lastResult = super.getSplits(job);
    lastResult
  }
}
