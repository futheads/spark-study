package com.futhead.scala.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by futhead on 17-7-13.
  */
object SimpleApp {

  def main(args: Array[String]): Unit = {
    val logFile = "/home/futhead/app/spark-2.0.2-bin-hadoop2.6/README.md"

    val conf = new SparkConf().setMaster("local").setAppName("Simple Application")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")

    spark.stop()
  }

}
