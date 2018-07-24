package com.futhead.scala.gls

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object AddId {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("add_id").master("local").getOrCreate()
    val sqlContext = spark.sqlContext

    val df_start = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", false.toString)
      .load("file:///E:/gls/clean/data/frank_origin_data.csv")


    val rowRdd = df_start.select("BILL_OF_LADING_NO").rdd
      .map{x => x.toString().substring(1,x.toString().length - 1)}
      .zipWithIndex()
        .map(a => Row(a._1, a._2))
      val schema = StructType(
        Array(
          StructField("BILL_OF_LADING_NO", StringType, true),
          StructField("id", LongType, true)
        )
      )
    val df_result = spark.createDataFrame(rowRdd, schema)
    df_result.show(false)
    spark.close()
  }
}
