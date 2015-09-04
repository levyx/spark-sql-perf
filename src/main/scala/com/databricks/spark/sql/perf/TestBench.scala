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
    val dataLocation="/tmp/mnt/ssd/tpc-ds"
    val tpcPath = "/Users/hamid/tpc/tools"
    val resultsLocation = "/tmp/results"


    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = Tables(sqlContext)
    // Setup TPC-DS experiment
    val tpcds =
      new TPCDS(
        sqlContext = sqlContext,
        databaseName = "parquet",
        sparkVersion = "1.4.0",
        dataLocation = dataLocation,
        dsdgenDir = tpcPath,
        tables = tables.tables,
        scaleFactor = "1")

    tpcds.setup()
    val experiment = tpcds.runExperiment(queries.impalaKitQueries, resultsLocation, iterations=1)
    experiment.waitForFinish(Int.MaxValue)

    // Get experiments results.

    val results = Results(resultsLocation = resultsLocation , sqlContext = sqlContext)
    // Get the DataFrame representing all results stored in the dir specified by resultsLocation.
    val allResults = results.allResults
    // Use DataFrame API to get results of a single run.
    //allResults.filter("timestamp = 1429132621024")
    println("[")
    allResults.toJSON.collect().foreach(row => println(row+","))
    println("]")

  }
}
