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

/**
 * Comparator used for terasort. It compares the first ten bytes of the record by
 * first comparing the first 8 bytes, and then the last 2 bytes (using unsafe).
 */
class TeraSortRecordOrdering extends Ordering[Array[Byte]] {

  override def compare(a: Array[Byte], b: Array[Byte]): Int = {
    if (a eq null) {
      if (b eq null) 0
      else -1
    }
    else if (b eq null) 1
    else {
      val L = math.min(a.length, b.length)
      var i = 0
      while (i < L) {
        if (a(i) < b(i)) return -1
        else if (b(i) < a(i)) return 1
        i += 1 
      }
      if (L < b.length) -1 
      else if (L < a.length) 1
      else 0
    }
  }
}
