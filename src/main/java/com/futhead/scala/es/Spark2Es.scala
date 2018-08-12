package com.futhead.scala.es

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.immutable.IntMap.Tip

/**
  * Created by Administrator on 2018/6/25.
  */
object Spark2Es {

  case class Trip(departure: String, arrival: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Spark2Es").setMaster("local[*]").set("es.nodes", "localhost:9200")
    conf.set("es.index.auto.create", "true");

    //    val conf = ...
    val sc = new SparkContext(conf)

    val filePath = "E:\\spark_test\\text.txt"

//    val lines = sc.textFile(filePath)
//    val rdd = lines.map(line => {
//      Trip("", "")
//    })
//    val lists = new util.ArrayList[Trip]()

//    upcomingTrip, lastWeekTrip
    val list =  List()
//    val ll = List()
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")
    val ll = list.:+(upcomingTrip).:+(lastWeekTrip)
    val rdd:RDD[Trip] = sc.makeRDD(ll)
    EsSpark.saveToEs(rdd, "spark/docs")

    sc.stop()
  }
}
