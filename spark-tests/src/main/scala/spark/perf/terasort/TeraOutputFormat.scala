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

/**
 * This file is copied from Hadoop package org.apache.hadoop.examples.terasort.
 */


import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 * An output format that writes the key and value appended together.
 */

object TeraOutputFormat {
  val FINAL_SYNC_ATTRIBUTE : String = "mapreduce.terasort.final.sync";
  val OUTDIR = "mapreduce.output.fileoutputformat.outputdir"
}

class TeraOutputFormat extends FileOutputFormat[Array[Byte], Array[Byte]] {
  var committer : OutputCommitter = null;

  /**
   * Set the requirement for a final sync before the stream is closed.
   */
  def setFinalSync(job : JobContext, newValue : Boolean) =
    job.getConfiguration().setBoolean(TeraOutputFormat.FINAL_SYNC_ATTRIBUTE, newValue);

  /**
   * Does the user want a final sync at close?
   */
  def getFinalSync(job : JobContext ) : Boolean =
    job.getConfiguration().getBoolean(TeraOutputFormat.FINAL_SYNC_ATTRIBUTE, false);

  class TeraRecordWriter(val out : FSDataOutputStream, val job: JobContext) 
  extends RecordWriter[Array[Byte], Array[Byte]] {
    var finalSync = getFinalSync(job)

    def write(key : Array[Byte], value : Array[Byte]) = {
      this.synchronized {
        out.write(key, 0, key.length);
        out.write(value, 0, value.length);
      }
    }

    def close(context : TaskAttemptContext) = {
      if (finalSync) {
        out.hsync();
      }
      out.close();
    }
  }

  override def checkOutputSpecs(job : JobContext) = {
    // Ensure that the output directory is set
    val outDir : Path = getOutputPath(job);
    if (outDir == null) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
      Array[Path](outDir), job.getConfiguration());
  }

  /*
  Backported from Hadoop FileOutputPath from later versions than 1.0.4
   */
  def getOutputPath(job : JobContext ) : Path =  {
    job.getConfiguration().get(TeraOutputFormat.OUTDIR) match {
      case null => null
      case name => new Path(name)
    }
  }

  def getRecordWriter(job : TaskAttemptContext) 
  : RecordWriter[Array[Byte], Array[Byte]] = {
    val file : Path = getDefaultWorkFile(job, "");
    val fs : FileSystem = file.getFileSystem(job.getConfiguration());
    val fileOut : FSDataOutputStream = fs.create(file);
    new TeraRecordWriter(fileOut, job);
  }

  override def getOutputCommitter(context : TaskAttemptContext) : OutputCommitter = {
    if (committer == null) {
      val output = getOutputPath(context);
      committer = new FileOutputCommitter(output, context);
    }
    committer;
  }
}
