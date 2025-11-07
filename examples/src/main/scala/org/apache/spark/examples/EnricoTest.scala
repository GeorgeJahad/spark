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

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object EnricoTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Enrico Test")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val n = 100000000
    val j = spark.sparkContext.broadcast(1000)
    val x = spark.range(0, n, 1, 100).select(col("id").cast("int"))
    x.as[Int]
      .mapPartitions { it => if (it.hasNext && it.next < n / 100 * 80) Thread.sleep(2000); it }
      .groupBy(col("value") % 1000).as[Int, Int]
      .flatMapSortedGroups(col("value")){ case (m, it) => if (it.hasNext && it.next == 0) Thread.sleep(10000); it }
      .write.mode(SaveMode.Overwrite).csv("/tmp/spark.csv")
    Thread.sleep(60000)
    println("gbj Test stopping")
    spark.stop()
  }
}
// scalastyle:on println
