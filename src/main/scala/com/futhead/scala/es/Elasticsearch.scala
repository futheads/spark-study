package com.futhead.scala.es

import com.futhead.scala.sql.SparkSQL.Person
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object Elasticsearch {

  case class Trip(departure: String, arrival: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkSQL")
      .setMaster("local[*]")
      .set("es.nodes", "node118:9200")
      .set("es.index.auto.create", "true")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val peopleRDD = spark.sparkContext
      .textFile("file:///D:\\program\\spark-2.1.2-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))

    EsSpark.saveToEs(peopleRDD, "spark/people")
  }
}
