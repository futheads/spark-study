package com.futhead.scala.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark

object SparkSQL {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkSQL")
      .setMaster("local[*]")
      .set("es.nodes", "node118:9200")
      .set("es.index.auto.create", "true")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    val df = spark.read.json("file:///home/futhead/data/spark/scala/people.json")

    //导入隐饰操作，否则RDD无法调用toDF方法
    val peopleRDD = spark.sparkContext
      .textFile("file:///D:\\program\\spark-2.1.2-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))

//    import spark.implicits._
//    val peopleRDD = spark.sparkContext.textFile("D:\\program\\spark-2.1.2-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
//    val schemaString = "name age"
//    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
//    val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
//    val peopleDF = spark.createDataFrame(rowRDD, schema)
//    peopleDF.createTempView("people")
//    val results = spark.sql("select name, age From people")
//    results.map(attributes => "name: " + attributes(0) + ", age:" + attributes(1)).show()

    EsSpark.saveToEs(peopleRDD, "spark/people")
  }
}
