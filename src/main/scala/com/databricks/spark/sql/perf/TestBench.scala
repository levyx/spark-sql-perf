package com.databricks.spark.sql.perf

import com.databricks.spark.sql.perf.tpcds._
import org.apache.spark.sql.parquet.Tables
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by hamid on 8/16/15.
 */
object TestBench {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetTest").setMaster("local[*]")

    var dataLocation ="/mnt/ssd/tpc-ds"
    var tpcPath = "/mnt/hdfs/TPCDSVersion1.3.1/tools"
    var resultsLocation = "/results"
    var databaseName = "xenon"
    var scaleFactor = "5"

    for (arg <- args) {
      arg match {
        case "-p" => dataLocation    = args(args.indexOf(arg) + 1)
        case "-t" => tpcPath         = args(args.indexOf(arg) + 1)
        case "-r" => resultsLocation = args(args.indexOf(arg) + 1)
        case "-d" => databaseName    = args(args.indexOf(arg) + 1)
        case "-s" => scaleFactor     = args(args.indexOf(arg) + 1)
        case _    =>
      }
    }

    println("\n\nConfiguration:\n")
    println("%40s".format("Data Path: [-p]") + "\t" + dataLocation)    
    println("%40s".format("TPC-DS Path: [-t]") + "\t" + tpcPath)
    println("%40s".format("results Path: [-r]") + "\t" + resultsLocation)
    println("%40s".format("Database namen: [-d]") + "\t" + databaseName)
    println("%40s".format("scale Factor: [-s]") + "\t" + scaleFactor)   
    println()



    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Tables in TPC-DS benchmark used by experiments.
    val tables = Tables(sqlContext)
    // Setup TPC-DS experiment
    val tpcds =
      new TPCDS(
        sqlContext = sqlContext,
        databaseName = databaseName,
        sparkVersion = "1.4.0",
        dataLocation = dataLocation,
        dsdgenDir = tpcPath,
        tables = tables.tables,
        scaleFactor =scaleFactor)

    tpcds.setup()
    val experiment = tpcds.runExperiment(queries.impalaKitQueries, resultsLocation, iterations=1)
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

    println()
    allResults.select("results.queryResponse").collect().foreach(
      s => {
        println(s)
        println("~~~~~~~~~~~~~~~~~~~~~~~~")
      }
    )
  }
}
