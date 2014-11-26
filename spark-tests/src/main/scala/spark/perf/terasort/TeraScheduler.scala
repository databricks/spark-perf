/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.terasort;

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream
import java.util.ArrayList


import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig

import com.google.common.base.Charsets

object TeraScheduler {
  val USE : String = "mapreduce.terasort.use.terascheduler"
  val LOG : Log = LogFactory.getLog(classOf[TeraScheduler])

  def getFileSize(path : Path, conf : Configuration) : Long = {
    val fs = path.getFileSystem(conf)
    val cSummary = fs.getContentSummary(path)
    cSummary.getLength
  }

  def main(args : Array[String]) = {
    val conf = new SparkConf()
      .setAppName(s"TeraScheduler")
    val sc = new SparkContext(conf)

    val inputFile = args(0)

    val hosts = args.slice(1, args.length)
    println(s"Calculating splits on $inputFile")

    val inputPath = new Path(inputFile)
    val fileSize = getFileSize(inputPath, sc.hadoopConfiguration)
    val splits = Array[FileSplit](new FileSplit(new Path(inputFile), 0, fileSize, hosts))
    val problem = new TeraScheduler(splits, sc.hadoopConfiguration)
    for (host <- hosts) {
      problem.hosts.append(new problem.Host(host))
      println(host)
    }
    LOG.info("starting solve")
    problem.solve()

    val leftOvers = problem.splits.filterNot{split =>
      split.isAssigned
    }
    for (split <- problem.splits) {
      if (split.isAssigned) println(s"sched: $split")
    }

    for (split <- leftOvers) {
      println(s"left: $split")
    }

    println("left over: " + leftOvers.length)
    LOG.info("done")
  }
}

class TeraScheduler(realSplits : Array[FileSplit], conf : Configuration) {

  val hosts = new ArrayList[Host]()
  var slotsPerHost : Int = conf.getInt(TTConfig.TT_MAP_SLOTS, 4)
  var remainingSplits : Int = 0
  var splits = new Array[Split](realSplits.length)
  this.calcSplits

  private def calcSplits() = {
    println("calcSplits")
    val hostTable = new HashMap[String, Host]()
    realSplits.foreach{ realSplit =>
      val split : Split = new Split(realSplit.getPath().toString())
      splits(remainingSplits) = split
      remainingSplits = remainingSplits + 1
      println("realSplit.getLocations: " + realSplit.getLocations)
      for(hostname <- realSplit.getLocations) {
        println(s"hostname: $hostname")
        var host : Host = null
        hostTable.get(hostname) match {
          case Some(h) => host = h
          case None => {
            host = new Host(hostname)
            hostTable.put(hostname, host)
            hosts.append(host)
          }
        }
        host.splits.append(split)
        split.locations.append(host)
        println(s"Appended split: $split to host: $host")
      }
    }
  }

  class Split(val filename: String) {
    var isAssigned : Boolean = false
    val locations = new ArrayList[Host]()

    override def toString() : String = {
      val result : StringBuffer = new StringBuffer()
      result.append(filename)
      result.append(" on ")
      locations.foreach{ host =>
        result.append(host.hostname)
        result.append(", ")
      }
      result.toString
    }
  }

  case class Host (val hostname : String) {
    val splits = new ArrayList[Split]()

    override def toString() : String = {
      val result = new StringBuffer();
      result.append(splits.size)
      result.append(" ")
      result.append(hostname)
      result.toString()
    }
  }

  def pickBestHost() : Host = {
    var result : Host = null;
    var splits : Int = Integer.MAX_VALUE;
    for(host <- hosts) {
      if (host.splits.size < splits) {
        result = host
        splits = host.splits.size
      }
    }
    if (result != null) {
      hosts.remove(result)
      TeraScheduler.LOG.debug("picking " + result)
    }
    result
  }

  def pickBestSplits(host : Host) {
    val splitsPerHost = Math.ceil((remainingSplits / hosts.size).asInstanceOf[Double])
    val tasksToPick = Math.min(slotsPerHost, splitsPerHost).asInstanceOf[Int]
    val best = new Array[Split](tasksToPick)
    for(cur <- host.splits) {
      TeraScheduler.LOG.debug("  examine: " + cur.filename + " " + cur.locations.size)
      println("  examine: " + cur.filename + " " + cur.locations.size)
      var i : Int = 0;
      while (i < tasksToPick && best(i) != null && 
             best(i).locations.size <= cur.locations.size) {
        i += 1;
      }
      println (s"i < tasksToPick? --> $i $tasksToPick")
      if (i < tasksToPick) {
        for(j <- tasksToPick - 1 to i by -1) {
          best(j) = best(j-1);
        }
        best(i) = cur;
      }
    }
    // for the chosen blocks, remove them from the other locations
    best.foreach{ b =>
      if (b != null) {
        TeraScheduler.LOG.debug(" best: " + b.filename)
        println(" best: " + b.filename)
        for (other <- b.locations) {
          other.splits.remove(b)
        }
        b.locations.clear()
        b.locations.append(host)
        b.isAssigned = true
        remainingSplits -= 1
      }
    }
    // for the non-chosen blocks, remove this host
    for(cur <- host.splits) {
      if (!cur.isAssigned) {
        cur.locations.remove(host)
      }
    }
  }
  
  def solve() = {
    var host : Host = pickBestHost()
    while (host != null && hosts.size != 0) {
      pickBestSplits(host)
      host = pickBestHost()
    }
  }

  /**
   * Solve the schedule and modify the FileSplit array to reflect the new
   * schedule. It will move placed splits to front and unplacable splits
   * to the end.
   * @return a new list of FileSplits that are modified to have the
   *    best host as the only host.
   * @throws IOException
   */
  def getNewFileSplits() : List[InputSplit] = {
    solve()
    val result = new Array[FileSplit](realSplits.length)
    var left : Int = 0
    var right : Int = realSplits.length - 1
    for(i <- 0 to splits.length) {
      if (splits(i).isAssigned) {
        // copy the split and fix up the locations
        val newLocations = Array[String](splits(i).locations.head.hostname)
        realSplits(i) = new FileSplit(realSplits(i).getPath(),
            realSplits(i).getStart(), realSplits(i).getLength(), newLocations)
        result(left) = realSplits(i)
        left = left + 1
      } else {
        result(right) = realSplits(i)
        right = right -1
      }
    }
    val ret = new ArrayList[InputSplit]()
    for (split <- result) { ret.append(split) } 
    ret.toList
  }
}
