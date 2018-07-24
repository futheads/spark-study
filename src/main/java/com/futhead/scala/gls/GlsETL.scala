package com.futhead.scala.gls

import java.util.regex.Pattern

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.elasticsearch.spark.sql._

import scala.util.control.Breaks.{break, breakable}

object GlsETL {

  var did2dnameMap = scala.collection.mutable.Map[String, String]()
  var level1Map = scala.collection.mutable.Map[String, Row]()
  var level2Map = scala.collection.mutable.Map[String, Row]()
  var level3Map = scala.collection.mutable.Map[String, Row]()

  var us_states = scala.Array[Row]()

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GlsETL")
      .set("es.nodes", "node118:9200")
      .set("es.index.auto.create", "true")
      .set("fs.default.name", "master113:8020")
      .set("HADOOP_USER_NAME", "bigdata")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sqlContext = spark.sqlContext

    val dst = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", false.toString)
      .load("hdfs://master113:8020/user/bigdata/metadata/district_for_addext.csv").toDF()

    us_states = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", false.toString)
      .load("hdfs://master113:8020/user/bigdata/metadata/district_us_states_abb.csv").toDF().collect()

    did2dnameMap = getDid2NameMap(dst)
    level1Map = getMapFromDataframe(dst.filter(dst("level") === "1"))
    level2Map = getMapFromDataframe(dst.filter(dst("level") === "2"))
    level3Map = getMapFromDataframe(dst.filter(dst("level") === "3"))

//    val df_start = spark.sql("select * from origin_bill")
    val df_start = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", false.toString)
      .load("hdfs://master113:8020/user/bigdata/metadata/frank_origin_data.csv")
    df_start.createOrReplaceTempView("df_start")

    val df_hs_code = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", false.toString)
      .load("hdfs://master113:8020/user/bigdata/metadata/product_new.csv")

    df_hs_code.createOrReplaceTempView("df_hs_code")

    spark.udf.register("getShortName", (name: String) => getShortName(name))
    spark.udf.register("getDescTop250", (description: String) => getDescTop250(description))
    spark.udf.register("address_extract", (description: String) => address_extract(description))

    val df_one = sqlContext.sql("select " +
      "BILL_OF_LADING_NO as frankly_code, " +
      "ACTUAL_ARRIVAL_DATE as frankly_time, " +
      "TEU as total_volume, " +
      "WEIGHT_IN_KG as total_weight, " +
      "GLS_HS_CODE as hs_code, " +
      "getShortName(FOREIGN_SHIPPER_NAME) as suppliers_short, " +
      "FOREIGN_SHIPPER_NAME as suppliers_name, " +
      "FOREIGN_SHIPPER_ADDRESS as suppliers_address, " +
      "getShortName(USA_CONSIGNEE_NAME) as businesses_short, " +
      "USA_CONSIGNEE_NAME as businesses_name, " +
      "CONSIGNEE_ADDRESS as businesses_address, " +
      "getShortName(CARRIER_CASC_CODE) as vessel_short, " +
      "CARRIER_CASC_CODE as vessel_name, " +
      "getDescTop250(PRODUCT_DESCRIPTION) as product_description " +
      "from df_start " +
      "where PRODUCT_DESCRIPTION != '' " +
      "and USA_CONSIGNEE_NAME != '' " +
      "and FOREIGN_SHIPPER_NAME != '' " +
      "and RECORD_STATUS != 'D'")
      .dropDuplicates()
      .withColumn("total_volume", col("total_volume").cast(FloatType))
      .withColumn("total_weight", col("total_weight").cast(FloatType) * 0.001)
      .filter(functions.col("total_weight").gt(0)).cache()
    df_one.createTempView("df_one")

    val df_businesses = spark.sql("select businesses_short as name, businesses_name as origin_name, " +
      "businesses_address as details_address, address_extract(concat(businesses_name, ' ', businesses_address)) as mixed, " +
      "product_description as last_product_desc, frankly_time as last_date from df_one order by frankly_time desc")
      .dropDuplicates("name")
      .withColumn("mixed_add", split(col("mixed"), "_")).select(
      col("name"),
      col("origin_name"),
      col("details_address"),
      col("last_date"),
      col("last_product_desc"),
      col("mixed_add").getItem(0).as("dname_level1"),
      col("mixed_add").getItem(1).as("did_level1"),
      col("mixed_add").getItem(2).as("dname_level2"),
      col("mixed_add").getItem(3).as("did_level2"),
      col("mixed_add").getItem(4).as("dname_level3"),
      col("mixed_add").getItem(5).as("did_level3")
    )

    df_businesses.createTempView("df_businesses")

    val df_suppliers = spark.sql("select suppliers_short as name, suppliers_name as origin_name, " +
      "suppliers_address as details_address, address_extract(concat(suppliers_name, ' ', suppliers_address)) as mixed, " +
      "product_description as last_product_desc, frankly_time as last_date from df_one")
      .dropDuplicates("name")
      .withColumn("mixed_add", split(col("mixed"), "_")).select(
      col("name"),
      col("origin_name"),
      col("details_address"),
      col("last_date"),
      col("last_product_desc"),
      col("mixed_add").getItem(0).as("dname_level1"),
      col("mixed_add").getItem(1).as("did_level1"),
      col("mixed_add").getItem(2).as("dname_level2"),
      col("mixed_add").getItem(3).as("did_level2"),
      col("mixed_add").getItem(4).as("dname_level3"),
      col("mixed_add").getItem(5).as("did_level3")
    )

    df_suppliers.createTempView("df_suppliers")

    val df_vessel = spark.sql("select vessel_short as name, vessel_name as origin_name from df_one")

    df_suppliers.createTempView("df_vessel")

    val df_frank = spark.sql("select " +
      "t.frankly_code, t.frankly_time, t.businesses_short, t.suppliers_short, t.vessel_short, t.total_volume, t.total_weight, t.product_description, " +
      "b.dname_level1 as businesses_dname_level1, " +
      "b.did_level1 as businesses_did_level1, " +
      "b.dname_level2 as businesses_dname_level2, " +
      "b.did_level2 as businesses_did_level2, " +
      "b.dname_level3 as businesses_dname_level3, " +
      "b.did_level3 as businesses_did_level3, " +
      "s.dname_level1 as suppliers_dname_level1, " +
      "s.did_level1 as suppliers_did_level1, " +
      "s.dname_level2 as suppliers_dname_level2, " +
      "s.did_level2 as suppliers_did_level2, " +
      "s.dname_level3 as suppliers_dname_level3, " +
      "s.did_level3 as suppliers_did_level3, " +
      "h.name as category_name, " +
      "h.category_level1_id, h.category_level1_name " +
      "from df_one t left join df_businesses b on t.businesses_short = b.name " +
      "left join df_suppliers s on t.suppliers_short = s.name " +
      "left join df_vessel v on t.vessel_short = v.name " +
      "left join df_hs_code h on t.hs_code = h.hs_code")
    df_frank.createTempView("df_frank")

    df_frank.saveToEs("spark/frank", Map("es.mapping.id" -> "frankly_code"))

    //    val df_businesses_static = spark.sql("select businesses_short, sum(total_weight) as total_weight, " +
    //      "sum(total_volume) as total_volume, count(distinct frankly_code) as total_count " +
    //      "from df_frank group by businesses_short")
    //    df_businesses_static.join(df_businesses, df_businesses_static("businesses_short") === df_businesses("name")).drop("businesses_short")
    //
    //    val df_suppliers_static = spark.sql("select suppliers_short, sum(total_weight) as total_weight, " +
    //      "sum(total_volume) as total_volume, count(distinct frankly_code) as total_count " +
    //      "from df_frank group by suppliers_short")
    //    df_suppliers_static.join(df_suppliers, df_suppliers_static("suppliers_short") === df_suppliers("name")).drop("suppliers_short").show()
    //
    //    spark.sql("select businesses_did_level1, businesses_did_level2, businesses_did_level3, " +
    //      "category_level1_id, frankly_time, suppliers_did_level1, suppliers_did_level2, " +
    //      "suppliers_did_level3, total_volume, total_weight, SUBSTRING(frankly_time, 0, 7) as month from df_frank")
    spark.close()
  }

  /**
    * 获取公司名的简称
    * @param name
    * @return
    */
  def getShortName(name: String) = {
    val regEx = "[0123456789_ `~!@#$%^&*()+=|{}':;',//[//].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"
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
    * 描述长过250个字符的就取前250个字符
    * @param text
    * @return
    */
  def getDescTop250(text: String): String = {
    if ("" == text) {
      return "UNAVAILABLE"
    }
    if (text.length > 250) {
      return text.substring(0, 250)
    }
    text
  }

  /**
    * 抽取地址
    * @param address
    * @return
    */
  def address_extract(address: String): String = {
    val US_NAME = List("USA", "U.S.A", "UNITED STATES", "US", "U S A")

    var level1 = "0"
    var level2 = "0"
    var level3 = "0"
    var level1_did = "0"
    var level2_did = "0"
    var level3_did = "0"

    val upAddress = address.replace(",", " ").replace(".", " ").toUpperCase()

    // flag1: level1是否已经匹配
    var flag1 = 0
    // flag_2: lwvel2是否已经匹配
    var flag2 = 0

    if(match_address("CN", upAddress)) {
      level1 = "CHINA"
      level1_did = "1"
      flag1 = 1
    }

    // 如果中国没有匹配到，那就试试美国
    if(flag1 == 0) {
      breakable {
        for(us <- US_NAME) {
          if(match_address(us, upAddress)) {
            level1 = "UNITED STATES"
            level1_did = "2434"
            flag1 = 1
            break
          }
        }
      }
    }

    // level1匹配, 非中美的一级地址
    if(flag1 == 0) {
      breakable{
        for(key <- level1Map.keys) {
          if (match_address(key, upAddress)) {
            level1 = key
            level1_did = level1Map(key).get(0).toString
            flag1 = 1
            break
          }
        }
      }
    }

    // level2匹配
    breakable {
      for((key, value) <- level2Map) {
        if (match_address(key, upAddress)) {
          if (level1 == "0") {
            level2 = key
            level2_did = value.get(0).toString
            level1_did = value.get(2).toString
            level1 = did2dnameMap(level1_did)
            flag1 = 1
            flag2 = 1
          } else {
            val level1_did_tmp = value.get(2).toString
            if(level1_did_tmp == level1_did) {
              level2 = key
              level2_did = value.get(0).toString
              flag1 = 1
              flag2 = 1
            }
            break
          }
        }
      }
    }

    // 如果第一循环判断的是美国，或者没有匹配的，就进行第二次循环
    if(level1_did == "2434" || flag1 == 0) {
      breakable {
        for(state <- us_states) {
          val did = state.get(1).toString
          val name = state.get(2).toString
          val abb = state.get(3).toString
          if (abb != "CO") {
            if(match_address(name, upAddress) || match_address(abb, upAddress)) {
              level1= "UNITED STATES"
              level1_did = "2434"
              level2 = name
              level2_did = did
              flag1 = 1
              flag2 = 1
              break
            }
          }
        }
      }
    }

    // level3
    if (level1 == "0" && level2 == "0") {
      return level1 + "_" + level1_did + "_" + level2 + "_" + level2_did + "_" + level3 + "_" + level3_did
    }

    // level3匹配
    breakable {
      for ((key, value) <- level3Map) {
        if(match_address(key, upAddress)) {
          val level2_did_tmp = value.get(2).toString
          val level1_did_tmp = level2Map(did2dnameMap(level2_did_tmp)).get(2).toString
          if (level1 != "0" && level2 == "0") {
            if (level1_did_tmp == level1_did) {
              level3 = key
              level3_did = value.get(0).toString
              level2_did = level2_did_tmp
              level2 = did2dnameMap(level2_did_tmp)
              level1_did = level1_did_tmp
              level1 = did2dnameMap(level1_did_tmp)
              break
            }
          } else if(level2 != "0") {
            if (level2_did_tmp == level2_did) {
              level3 = key
              level3_did = value.get(0).toString
              level2_did = level2_did_tmp
              level2 = did2dnameMap(level2_did)
              level1_did = level1_did_tmp
              level1 = did2dnameMap(level1_did)
              break
            }
          }
        }
      }
    }
    level1 + "_" + level1_did + "_" + level2 + "_" + level2_did + "_" + level3 + "_" + level3_did
  }

  /**
    * 获取did -> dname的Map
    * @param df
    * @return
    */
  def getDid2NameMap(df: DataFrame): scala.collection.mutable.Map[String, String] = {
    val list = df.collect()
    val map = scala.collection.mutable.Map[String, String]()
    for(row <- list) {
      map += (row(0).toString -> row(1).toString)
    }
    map
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

  /**
    * 判断单词是否在地址中
    * @param word
    * @param address
    * @return
    */
  def match_address(word: String, address: String) = {
    (" " + address + " ").contains(" " + word + " ")
  }

}
