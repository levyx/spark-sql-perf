package com.databricks.spark.sql.perf

import java.io.File

import com.databricks.spark.sql.perf.tpcds._
import org.apache.spark.sql.execution.datasources.parquet.Tables
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.immutable.Map
import scala.util.parsing.json._


/**
 * Created by hamid on 8/16/15.
 */
object TestBench {
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def extractInfo(pre: String, result: Option[Any]): List[(String,String)] = {
    result match {
      case Some(e) => e.asInstanceOf[Map[String, Any]].toList.flatMap {
        case (k, v) => v match {
          case h: Map[String, Any] => extractInfo(pre+k+".",Some(v))
          case l: List[Any] => Nil
          case _ => List((pre+k, v match {case s:String => s.replaceAll("\\n","\\\\n"); case i:Double => "%1.4f".format(i).replaceAll("\\.0+$","")}))
        }
      }
      case _ => List[(String, String)]()
    }
  }

  def getResultRecords(result: Option[Any]): List[List[(String,String)]] = {
    result match {
      case Some(e) =>
        e.asInstanceOf[Map[String, Any]].getOrElse("results",List[Any]()).asInstanceOf[List[Any]].map {
          i =>
            val timeStamp: Double = e.asInstanceOf[Map[String, Double]].getOrElse("timestamp",0)
            ("timestamp","%1.4f".format(timeStamp).replaceAll("\\.0+$","")) :: extractInfo("", Some(i))
        }
      case _ => List[List[(String, String)]]()
    }
  }



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetTest").setMaster("local[*]")

    var dataLocation ="/tmp/mnt/ssd/tpc-ds"
    var tpcPath = "/Users/hamid/tpc/tools"
    var resultsLocation = "/tmp/results"
    var databaseName = "xenon"
    var scaleFactor = "1"

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
        sparkVersion = "1.5.0",
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

    val firstResult = JSON.parseFull(allResults.toJSON.collect().head)
    val a = extractInfo("", firstResult)

    printToFile(new File("config.csv")) { p =>
      p.println(a.map(r => r._1).reduce((s,r)=> s+"|"+r))

      allResults.toJSON.collect().foreach { result =>
        val a = extractInfo("", JSON.parseFull(result))
        p.println(a.map(r => r._2).reduce((s, r) => s + "|" + r))
      }
    }

    printToFile(new File("results.csv")) { p =>
    val b = getResultRecords(firstResult).head
      p.println(b.map(r => r._1).reduce((s,r)=> s+"|"+r))
      allResults.toJSON.collect().foreach { result =>
        val b = getResultRecords(JSON.parseFull(result))
        b.foreach{row => p.println(row.map(r => r._2).reduce((s, r) => s + "|" + r))}
      }
    }

  }
}
