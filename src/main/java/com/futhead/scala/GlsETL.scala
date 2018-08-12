package com.futhead.scala

import java.util.regex.{Matcher, Pattern}

import org.apache.hadoop.hive.ql.exec.Utilities.SQLCommand
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
  * Created by Administrator on 2018/6/30.
  */
object GlsETL {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("GlsETL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    getDistrictMap(spark)

    //    val sparkContext = new SparkContext(sparkConf)

//    val name = "heloo, sdavck<>?asdfsf!#~%^^)"
//    print(getShortName(name))

//    clean(spark)
  }

  def clean(spark: SparkSession): Unit = {

    val sqlContext = spark.sqlContext

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("file:///E:/gls/clean/data/frank_origin_data.csv")

    import spark.implicits._

//    注册UDF函数
//    spark.udf.register("get_time", (time: String) => time.substring(0, 11))
//    df_tmp.select($"frankly_code", callUDF("get_time", $"frankly_time")).toDF("frankly_code", "frankly_time").show()

    // 读入数据并坐基本处理
    val df_tmp = df.withColumn("frankly_time", col("frankly_time").cast(StringType))
        .withColumn("frankly_time", col("frankly_time").substr(0, 10))
        .withColumn("year", col("frankly_time").substr(0, 4))
        .withColumn("total_weight", col("total_weight") * 0.001)

    df_tmp.createOrReplaceTempView("df_tmp")

    spark.udf.register("getShortName", (name: String) => getShortName(name))

//    val Df2 = Df1.withColumn("splitcol",split(col("contents"), ",")).select(
//      col("splitcol").getItem(0).as("col1"),
//      col("splitcol").getItem(1).as("col2"),
//      col("splitcol").getItem(2).as("col3")
//    )

    sqlContext.sql("select *, getShortName(businesses_name) as businesses_short from df_tmp" )
      .withColumn("tmp", split(col("businesses_address"), "_"))
      .select(
        col("tmp").getItem(0).as("tmp_0"),
        col("tmp").getItem(1),
        col("tmp").getItem(2))
      .show()

    //过滤
//    val df_filter = df.dropDuplicates()
//      .withColumn("businesses_short", col("businesses_short"))
//    df_tmp.join(df_tmp.select(callUDF("getShortName", $"businesses_name")).toDF("businesses_short")).show()

//    df_tmp.show()

  }

  /**
    * 统一日期格式
    * @param text
    * @return
    */
  def getTime(text: String): String = {
    if(Pattern.compile("[0-9]*").matcher(text).matches()) {
      val year = text.substring(0, 4)
      val month = text.substring(4, 6)
      val day = text.substring(6, 8)
      return year + "-" + month + "-" + day
    } else {
      try {
        val date = text.split("-")
        val year = date(0)
        var month = date(1)
        var day = date(2)
        if(month.length == 1) {
          month = "0" + month
        }
        if (day.length == 1) {
          day = "0" + day
        }
        return year + "-" + month + "-" + day
      } catch {
        case e: Exception => "error: " + text + ", "  + e.getCause()
      }
    }
  }

  /**
    * 获取公司名的简称
    * @param name
    * @return
    */
  def getShortName(name: String) = {
    val regEx = "0123456789[`~!@#$%^&*()+=|{}':;',//[//].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"
    val pattern = Pattern.compile(regEx)
    val matcher = pattern.matcher(name)
    var upperName = matcher.replaceAll("").trim().toUpperCase
    val companySuffix = List("LTD", "CORPORATION", "INCORPORATED", "CORP", "COMPANY", "CO", "LCC", "LLC", "INC", "LIMITED")
    val companyPrefix = List("UNTOTHEORDEROF", "TOTHEORDEROF", "TOORDEROFSHIPPER", "TOORDEROF", "TOORDER", "ORDEROF", "TOTHEORDER", "ONBEHALFOF", "ASAGENTOF", "ASAGENT", "OBOF", "OB", "TOTHE")
    for (i <- companyPrefix) {
      if(upperName.startsWith(i)) {
        upperName.replace(i, "")
      }
    }
    for(i <- companySuffix) {
      if(upperName.endsWith(i)) {
        upperName.replace(i, "")
      }
    }
    val badName = List("NA", "NAN", "")
    if(badName.contains(upperName) || name.length < 2) {
      upperName = "UNAVAILABLE"
    }
    upperName
  }

  /**
    *
    * @param spark
    */
  def getDistrictMap(spark: SparkSession): Unit = {
    val sqlContext = spark.sqlContext
    val dst = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", true.toString)
      .load("file:///E:/gls/clean/data/district_for_addext.csv").toDF()
    val leve1_map = getMapFromDataframe(dst.filter(dst("level") === 1))
    val leve2_map = getMapFromDataframe(dst.filter(dst("level") === 2))
    val leve3_map = getMapFromDataframe(dst.filter(dst("level") === 3))
    for ((key, value) <- leve1_map) {
      println(key + " -----> " + value)
    }
//    print(distMap("CHINA"))
  }

  /**
    * 将Dataframe转为key为
    * @param df
    * @return
    */
  def getMapFromDataframe(df: DataFrame): scala.collection.mutable.Map[String, Row] = {
    val list = df.collect()
    val distMap = scala.collection.mutable.Map[String, Row]()
    for(row <- list) {
      distMap += (row(1).toString -> row)
    }
    distMap
  }

//  <dependency>
//    <groupId>org.elasticsearch</groupId>
//    <artifactId>elasticsearch-spark-20_2.11</artifactId>
//    <version>6.2.3</version>
//  </dependency>
}
