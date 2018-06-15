package com.futhead.scala.es

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object Elasticsearch {

  case class Trip(departure: String, arrival: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("import2es").setMaster("local[*]").set("es.nodes", "node118:9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(rdd, "spark/docs")

    sc.stop()
  }
}
