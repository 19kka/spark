/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.u51.marble

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import com.google.common.base.Stopwatch
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.junit.{BeforeClass, Test}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.direct.DirectPlanStrategies
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel

object SparkLocalBenchMark {
  @BeforeClass
  def setup(): Unit = {
    BenchMarkUtil.initDBTables()
  }
}

class SparkLocalBenchMark {

  val SPARK_WORK_DIR = "/tmp/spark"
  var spark: SparkSession = new SparkSession.Builder()
    .appName("spark-local-benchmark")
    .config("spark.ui.enabled", false)
    .config("spark.sql.warehouse.dir", SPARK_WORK_DIR)
    .config("hive.exec.scratchdir", SPARK_WORK_DIR + "/hive")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.default.parallelism", 1)
    .config("spark.sql.catalogImplementation", "direct")
    .config("spark.sql.codegen.wholeStage", false)
//    .config("spark.memory.offHeap.enabled", true)
//    .config("spark.memory.offHeap.size", 1024 * 1024 * 1024)
    .master("local[1]")
    .withExtensions(sessionExtensions =>
      DirectPlanStrategies.strategies.foreach(strategy =>
        sessionExtensions.injectPlannerStrategy(_ => strategy)))
    .getOrCreate()

  val dfCache = CacheBuilder
    .newBuilder()
    .maximumSize(SQLConf.get.codegenCacheMaxEntries)
    .build(new CacheLoader[String, DataFrame]() {
      override def load(sql: String): DataFrame = {
        spark.sql(sql)
      }
    })

  def sqlQuery(sql: String): Double = {
    val s = new Stopwatch().start()
    val resultTable = dfCache.get(sql).collectDirectly()
    println(resultTable.mkString("\n"))
    System.out.println("sqlQuery result table size:" + resultTable.length)
    s.stop
    s.elapsed(TimeUnit.MICROSECONDS) * 0.001
  }

  val connectOptions = Map(
    "url" -> BenchMarkUtil.DB_CONNECTION_URL,
    "user" -> BenchMarkUtil.DB_USER,
    "password" -> BenchMarkUtil.DB_PASSWORD,
    "driver" -> BenchMarkUtil.DB_DRIVER)

  def runSqlForSingleTable(limit: Int, sql: String): Double = {
    System.out.println("start runSqlForSingleTable")
    val s = new Stopwatch().start()
    spark = spark.newSession()
    spark.sqlContext.clearCache()
    val fetchSql = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit)
    val df1 = spark.read
      .format("jdbc")
      .options(connectOptions)
      .option("dbtable", s"($fetchSql) tmp ")
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
    spark
      .createDataFrame(df1.collect().toList.asJava, df1.schema)
      .createOrReplaceTempView("item1")
    s.stop
    println(s"fetch:${s.elapsed(TimeUnit.MICROSECONDS) * 0.001}")
    sqlQuery(sql)

  }

  def runSqlForJoin(limit: Int, sql: String): Double = {
    System.out.println("start runSqlForJoin")
    val s = new Stopwatch().start()
    spark = spark.newSession()
    spark.sqlContext.clearCache()
    val fetchSql1 = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit)
    val df1 = spark.read
      .format("jdbc")
      .options(connectOptions)
      .option("dbtable", s"($fetchSql1) tmp ")
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
    spark
      .createDataFrame(df1.collect().toList.asJava, df1.schema)
      .createOrReplaceTempView("item1")
    val fetchSql2 = BenchMarkUtil.generateFetchSql("item2", "i_item_sk", limit)
    val df2 = spark.read
      .format("jdbc")
      .options(connectOptions)
      .option("dbtable", s"($fetchSql2) tmp ")
      .load()
      .persist(StorageLevel.MEMORY_ONLY)
    spark
      .createDataFrame(df2.collect().toList.asJava, df2.schema)
      .createOrReplaceTempView("item2")
    s.stop
    println(s"fetch:${s.elapsed(TimeUnit.MICROSECONDS) * 0.001}")
    sqlQuery(sql)
  }

  @Test
  def testProjection(): Unit = {
    val sql = "SELECT substring(i_item_id,0,1) FROM  item1 where i_item_sk>1000"
//    runSqlForSingleTable(1, sql)
    //    System.out.println(runSqlForSingleTable(200, sql))
//    System.out.println(runSqlForSingleTable(2000, sql))
//    System.out.println(runSqlForSingleTable(4000, sql))
//    System.out.println(runSqlForSingleTable(10000, sql))
    System.out.println(runSqlForSingleTable(15000, sql))
    System.out.println(runSqlForSingleTable(15000, sql))

  }

  @Test
  def testEquiJoin(): Unit = {
    val sql =
      "SELECT t1.*,t2.*,substring('abc',1,2)  FROM item1 t1 INNER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk"
//    runSqlForJoin(1, sql)
//    System.out.println(runSqlForJoin(200, sql))
//    System.out.println(runSqlForJoin(2000, sql))
//    System.out.println(runSqlForJoin(4000, sql))
//    System.out.println(runSqlForJoin(10000, sql))
    System.out.println(runSqlForJoin(15000, sql))
    System.out.println(runSqlForJoin(15000, sql))

  }

  @Test
  def testThetaJoin(): Unit = {
    val sql =
      "SELECT t1.i_item_sk,t2.i_item_sk  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <15000"
    runSqlForJoin(1, sql)
//    System.out.println(runSqlForJoin(200, sql))
    //    System.out.println(runSqlForJoin(2000, sql))
    //    System.out.println(runSqlForJoin(4000, sql))
    //    System.out.println(runSqlForJoin(10000, sql))
    //    System.out.println(runSqlForJoin(15000, sql))
    System.in.read()

  }

  @Test
  def testAggrectgate(): Unit = {
    val sql =
      "SELECT count(*) ,sum(i_current_price),max(i_current_price),percentile_approx(i_current_price,0.8)  FROM item1 group by i_item_id "
//    runSqlForSingleTable(1, sql)
//    System.out.println(runSqlForSingleTable(2000, sql))
//    System.out.println(runSqlForSingleTable(4000, sql))
//    System.out.println(runSqlForSingleTable(10000, sql))
    System.out.println(runSqlForSingleTable(15000, sql))
    System.out.println(runSqlForSingleTable(15000, sql))

  }

  @Test
  def testComplexSql(): Unit = {
    val sql =
      "SELECT t4.* FROM (SELECT t3.i_item_id,count(t3.*) as count0 ,sum(t3.i_current_price) as sum0,max(t3.i_current_price) as max0  FROM (SELECT t1.*  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <15000) t3 group by t3.i_item_id ) t4 where t4.count0>2"
    runSqlForJoin(1, sql)
//    System.out.println(runSqlForJoin(15000, sql))
    System.in.read()

  }

}

// End SparkLocalBenchMark.java
