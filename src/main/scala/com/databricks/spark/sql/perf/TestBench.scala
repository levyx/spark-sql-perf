package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds._
import org.apache.spark.sql.parquet.Tables
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by hamid on 8/16/15.
 */
object TestBench {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetTest")
    val dataLocation="/mnt/ssd/tpc-ds"
    val tpcPath = "/mnt/hdfs/TPCDSVersion1.3.1/tools"
    val resultsLocation = "/results"


    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = Tables(sqlContext)
    // Setup TPC-DS experiment
    val tpcds =
      new TPCDS(
        sqlContext = sqlContext,
        databaseName = "xenon",
        sparkVersion = "1.4.0",
        dataLocation = dataLocation,
        dsdgenDir = tpcPath,
        tables = tables.tables,
        scaleFactor = "5")

    tpcds.setup()
    val experiment = tpcds.runExperiment(queries.xenonQueries, resultsLocation, iterations=1)
    experiment.waitForFinish(Int.MaxValue)
    println()
    println(" ============== Experiment status messages ==============")
    experiment.currentMessages.foreach(println)
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
