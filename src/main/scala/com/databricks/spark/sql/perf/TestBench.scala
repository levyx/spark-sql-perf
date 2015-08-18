package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds._
import org.apache.spark.sql.parquet.Tables
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by hamid on 8/16/15.
 */
object TestBench {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetTest").setMaster("local[8]")
    val tpcPath = "/Users/hamid/tpc/tools"


    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = Tables(sqlContext)
    // Setup TPC-DS experiment
    val tpcds =
      new TPCDS(
        sqlContext = sqlContext,
        sparkVersion = "1.3.0",
        dataLocation = tpcPath,
        dsdgenDir = tpcPath,
        tables = tables.tables,
        scaleFactor = "1")

    tpcds.setup()
    val experiment = tpcds.runExperiment(queries.impalaKitQueries, tpcPath)

  }
}
