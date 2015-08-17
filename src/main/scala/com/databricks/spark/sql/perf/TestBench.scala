package com.databricks.spark.sql.perf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.parquet.Tables
import com.databricks.spark.sql.perf.tpcds._


/**
 * Created by hamid on 8/16/15.
 */
object TestBench {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("ParquetTest").setMaster("local[1]")


    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Tables in TPC-DS benchmark used by experiments.
    val tables = Tables(sqlContext)
    // Setup TPC-DS experiment
    val tpcds =
      new TPCDS(
        sqlContext = sqlContext,
        sparkVersion = "1.3.0",
        dataLocation = "/Users/hamid/tpc/old/tools",
        dsdgenDir = "/Users/hamid/tpc/old/tools",
        tables = tables.tables,
        scaleFactor = "1")

    tpcds.setup()
    val experiment = tpcds.runExperiment(queries.impalaKitQueries,"/Users/hamid/tpc/old/tools")

  }
}
