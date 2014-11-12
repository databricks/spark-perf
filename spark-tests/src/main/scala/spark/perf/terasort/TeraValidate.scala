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

import org.apache.hadoop.io.Text;
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.spark.SparkContext._

/**
 * An application that reads sorted data according to the terasort spec and
 * reports if it's indeed sorted.
 * This is an example program to validate TeraSort results
 *
 * See http://sortbenchmark.org/
 */

object TeraValidate{

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("usage:")
      println("DRIVER_MEMORY=[mem] bin/run-example " +
        "org.apache.spark.examples.terasort.TeraValidate " +
        "[input-directory] [output-directory]")
      println(" ")
      println("example:")
      println("DRIVER_MEMORY=50g bin/run-example " +
        "org.apache.spark.examples.terasort.TeraValidate " +
        "file:///scratch/username/terasort_in ")
      System.exit(0)
    }

    // Process command line arguments
    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAppName(s"TeraValidate")
    val sc = new SparkContext(conf)

    println("Calculating input size...")
    // Read data from input directory
    val dataset = sc.newAPIHadoopFile[Array[Byte], Array[Byte], TeraInputFormat](inputFile)
    printf("Input size: %d\n", dataset.count())

    // Copy the data due to bugs like SPARK-1018: 
    // https://spark-project.atlassian.net/browse/SPARK-1018
    val output = dataset.map{ case (k,v) => (k.clone(), v.clone())}
      .mapPartitions( iter => {
        val ERROR = new Text("error")
        val CHECKSUM = new Text("checksum")
        val compare = new TeraSortRecordOrdering()
        var res = List.newBuilder[(Text, Text)]

        if (iter.isEmpty) {
          res.+=((new Text(inputFile + ":empty"), new Text("")))
          res.result().iterator
        }
        iter.sliding(2).foreach{ case Seq(prev, curr) => {
          if (compare.compare(prev._1, curr._1) < 0) {
            res.+=((ERROR,
              new Text("misorder in " + inputFile +
                " between " + prev._1.toString +
                " and " + curr._1.toString)))
          } 
        }}
        res.result().iterator
      })
    output.saveAsTextFile(outputFile)
    sc.stop()
  }
}
