package com.futhead.scala.gls

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, IntegerType}


object GLSstastic {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("GlsETL")
      .set("es.nodes", "node118:9200")
      .set("es.index.auto.create", "true")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    val df_statistics = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", false.toString)
      .load("file:///E:/gls/clean/data/statistic_data.csv")
      .withColumn("month", col("month").cast(IntegerType))
      .withColumnRenamed("category_level1_id", "cid")
      .withColumnRenamed("category_level2_id", "pid")
    df_statistics.createOrReplaceTempView("df_statistics")

    val prop = new Properties()
    prop.put("user", "geluosi")
    prop.put("password", "Geluosi!QAZ2wsx")
    prop.put("driver","com.mysql.jdbc.Driver")

    val dates = get_date()

    //统计did-did, 进口地区进口商数量和出口地区出口商数量
//    val df_district_coordinate = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("inferSchema", false.toString)
//      .load("file:///E:/gls/clean/data/district_coordinate.csv")
//    df_district_coordinate.createOrReplaceTempView("df_district_coordinate")
//
//    val company_sql_template = "select businesses_did_level1, count(distinct businesses_id) as businesses_count, " +
//      "suppliers_did_level1, count(distinct suppliers_id) as suppliers_count, %d as datetype from df_statistics " +
//      "where businesses_did_level1 != '0' and suppliers_did_level1 != '0' %s " +
//      "group by businesses_did_level1, suppliers_did_level1 "
//    var company_sqls = List[String]()
//    for (i <- 0 to 2) {
//      company_sqls = company_sqls ++ List(company_sql_template.format(i, " and month > %d ".format(dates(i))))
//    }
//    company_sqls = company_sqls ++ List(company_sql_template.format(3, ""))
//    val company_sql = company_sqls.mkString(" union all ")
//    val df_company = spark.sql(company_sql)
//    df_company.createOrReplaceTempView("df_company")
//
//    spark.sql("select c.*, d_b.dname_en as businesses_country, d_b.longitude as b_longitude, d_b.latitude as b_latitude, " +
//      "d_s.dname_en as suppliers_country, d_s.longitude as s_longitude, d_s.latitude as s_latitude from df_company c " +
//      "left join df_district_coordinate d_b on c.businesses_did_level1 = d_b.did " +
//      "left join df_district_coordinate d_s on c.suppliers_did_level1 = d_s.did")
//      .write.mode("append")
//      .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.statis_dist_company", prop)

    //统计采购商半年、一年、三年及全部的交易量
//    val df_businesses = spark.sql("select businesses_id, count(businesses_id) as total_count, sum(total_volume) as total_volume, sum(total_weight) as total_weight from df_statistics group by businesses_id")
//    val df_businesses_0 = spark.sql("select businesses_id as businesses_id_0, count(businesses_id) as total_count_0, sum(total_volume) as total_volume_0, sum(total_weight) as total_weight_0 from df_statistics where month > %d group by businesses_id ".format(dates(0)))
//    val df_businesses_1 = spark.sql("select businesses_id as businesses_id_1, count(businesses_id) as total_count_1, sum(total_volume) as total_volume_1, sum(total_weight) as total_weight_1 from df_statistics where month > %d group by businesses_id ".format(dates(1)))
//    val df_businesses_2 = spark.sql("select businesses_id as businesses_id_2, count(businesses_id) as total_count_2, sum(total_volume) as total_volume_2, sum(total_weight) as total_weight_2 from df_statistics where month > %d group by businesses_id ".format(dates(2)))
//
//    df_businesses.join(df_businesses_0, df_businesses("businesses_id") === df_businesses_0("businesses_id_0"), "left_outer").drop("businesses_id_0")
//      .join(df_businesses_1, df_businesses("businesses_id") === df_businesses_1("businesses_id_1"), "left_outer").drop("businesses_id_1")
//      .join(df_businesses_2, df_businesses("businesses_id") === df_businesses_2("businesses_id_2"), "left_outer").drop("businesses_id_2")
//      .write.mode("append")
//      .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.statis_businesses", prop)

    //统计供应商半年、一年、三年及全部的交易量
//    val df_suppliers = spark.sql("select suppliers_id, count(suppliers_id) as total_count, sum(total_volume) as total_volume, sum(total_weight) as total_weight from df_statistics group by suppliers_id")
//    val df_suppliers_0 = spark.sql("select suppliers_id as suppliers_id_0, count(suppliers_id) as total_count_0, sum(total_volume) as total_volume_0, sum(total_weight) as total_weight_0 from df_statistics where month > %d group by suppliers_id ".format(dates(0)))
//    val df_suppliers_1 = spark.sql("select suppliers_id as suppliers_id_1, count(suppliers_id) as total_count_1, sum(total_volume) as total_volume_1, sum(total_weight) as total_weight_1 from df_statistics where month > %d group by suppliers_id ".format(dates(1)))
//    val df_suppliers_2 = spark.sql("select suppliers_id as suppliers_id_2, count(suppliers_id) as total_count_2, sum(total_volume) as total_volume_2, sum(total_weight) as total_weight_2 from df_statistics where month > %d group by suppliers_id ".format(dates(2)))
//
//    df_suppliers.join(df_suppliers_0, df_suppliers("suppliers_id") === df_suppliers_0("suppliers_id_0"), "left_outer").drop("suppliers_id_0")
//      .join(df_suppliers_1, df_suppliers("suppliers_id") === df_suppliers_1("suppliers_id_1"), "left_outer").drop("suppliers_id_1")
//      .join(df_suppliers_2, df_suppliers("suppliers_id") === df_suppliers_2("suppliers_id_2"), "left_outer").drop("suppliers_id_2")
//      .write.mode(SaveMode.Overwrite)
//      .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.statis_suppliers", prop)

    val b_did_list = List("businesses_did_level1", "businesses_did_level2", "businesses_did_level3")
    val s_did_list = List("suppliers_did_level1", "suppliers_did_level2", "suppliers_did_level3")

    //按月份统计did-pid对应的进出口weight, valume
//    var b_did_sql_list = List[String]()
//    var s_did_sql_list = List[String]()
//
//    for (b_did <- b_did_list) {
//      b_did_sql_list = b_did_sql_list ++ List("select month, cid, pid,  %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume from df_statistics where %s != '0' group by cid, pid, month, %s ".format(b_did, b_did, b_did))
//      b_did_sql_list = b_did_sql_list ++ List("select month, 0 as cid, 0 as pid, %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume from df_statistics where %s != '0' group by month, %s ".format(b_did, b_did, b_did))
//    }
//    val df_prodcut_did = spark.sql(b_did_sql_list.mkString(" union all "))
//
//    for (s_did <- s_did_list) {
//      s_did_sql_list = s_did_sql_list ++ List("select month, cid, pid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume from df_statistics where %s != '0' group by cid, pid, month, %s ".format(s_did, s_did, s_did))
//      s_did_sql_list = s_did_sql_list ++ List("select month, 0 as cid, 0 as pid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume from df_statistics where %s != '0' group by month, %s ".format(s_did, s_did, s_did))
//    }
//    val df_prodcut_sid = spark.sql(s_did_sql_list.mkString(" union all "))
//
//    val df_product_0 = df_prodcut_did.join(df_prodcut_sid, Seq("month", "cid", "pid", "did"), "outer")
//
//    val df_did_0 = spark.sql("select month, cid, pid, 0 as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume from df_statistics group by cid, pid, month")
//    val df_month = spark.sql("select month, 0 as cid, 0 as pid, 0 as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume from df_statistics group by month")
//    df_did_0.union(df_month)
//      .withColumn("imp_total_weight", col("exp_total_weight"))
//      .withColumn("imp_total_volume", col("exp_total_volume"))
//      .union(df_product_0)
//      .withColumnRenamed("month", "s_date")
//      .write.mode(SaveMode.Append)
//      .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.statis_dis_product_month", prop)

    //按月统计did-cid对应的进出口total_volume, total_weight
//    var b_did_sql_list_cate_month = List[String]()
//    var s_did_sql_list_cate_month = List[String]()
//    for(b_did <- b_did_list) {
//      b_did_sql_list_cate_month = b_did_sql_list_cate_month ++ List("select month, cid,  %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume from df_statistics where %s != '0' group by cid, month, %s ".format(b_did, b_did, b_did))
//      b_did_sql_list_cate_month = b_did_sql_list_cate_month ++ List("select month, 0 as cid,  %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume from df_statistics where %s != '0' group by month, %s ".format(b_did, b_did, b_did))
//    }
//    val b_did_cate_month = spark.sql(b_did_sql_list_cate_month.mkString(" union all "))
//
//    for(s_did <- s_did_list) {
//      s_did_sql_list_cate_month = s_did_sql_list_cate_month ++ List("select month, cid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume from df_statistics where %s != '0' group by cid, month, %s ".format(s_did, s_did, s_did))
//      s_did_sql_list_cate_month = s_did_sql_list_cate_month ++ List("select month, 0 as cid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume from df_statistics where %s != '0' group by month, %s ".format(s_did, s_did, s_did))
//    }
//    val s_did_cate_month = spark.sql(s_did_sql_list_cate_month.mkString(" union all "))
//    val final_cate_month = b_did_cate_month.join(s_did_cate_month, Seq("month", "cid", "did"), "outer")
//
//    spark.sql("select month, cid, 0 as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume from df_statistics group by cid, month " +
//      "union all select month, 0 as cid, 0 as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume from df_statistics group by month ")
//      .withColumn("exp_total_weight", col("imp_total_weight"))
//      .withColumn("exp_total_volume", col("imp_total_volume"))
//      .union(final_cate_month)
//      .withColumnRenamed("month", "s_date")
//      .write.mode(SaveMode.Append)
//      .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.statis_dis_cate_month", prop)

    //统计did-cid对应的进出口
//    for (i <- 0 to 3) {
//      val date = dates(i)
//      var b_did_sql_list_cate = List[String]()
//      var s_did_sql_list_cate = List[String]()
//      for(b_did <- b_did_list) {
//        b_did_sql_list_cate = b_did_sql_list_cate ++ List("select cid, %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count from df_statistics where month > %s group by cid, %s ".format(b_did, date, b_did))
//        b_did_sql_list_cate = b_did_sql_list_cate ++ List("select 0 as cid, %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count from df_statistics where month > %s group by %s ".format(b_did, date, b_did))
//      }
//      b_did_sql_list_cate = b_did_sql_list_cate ++ List("select cid, 0 as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count from df_statistics where month > %s group by cid".format(date))
//      val df_b_did_cate = spark.sql(b_did_sql_list_cate.mkString(" union all "))
//
//      for(s_did <- s_did_list) {
//        s_did_sql_list_cate = s_did_sql_list_cate ++ List("select cid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume, count(distinct suppliers_id) as supp_count from df_statistics where month > %s group by cid, %s ".format(s_did, date, s_did))
//        s_did_sql_list_cate = s_did_sql_list_cate ++ List("select 0 as cid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume, count(distinct suppliers_id) as supp_count from df_statistics where month > %s group by %s ".format(s_did, date, s_did))
//      }
//      s_did_sql_list_cate = s_did_sql_list_cate ++ List("select cid, 0 as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume, count(distinct suppliers_id) as supp_count from df_statistics where month > %s group by cid".format(date))
//      val df_s_did_cate = spark.sql(s_did_sql_list_cate.mkString(" union all "))
//
//      val df_all_cate = spark.sql("select 0 as cid, 0 as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count, count(distinct suppliers_id) as supp_count from df_statistics")
//        .withColumn("exp_total_weight", col("imp_total_weight"))
//        .withColumn("exp_total_volume", col("imp_total_volume"))
//
//      var table_name = "statis_dis_cate_" + i
//      if (i == 3) {
//        table_name = "statis_dis_cate"
//      }
//
//      spark.sql(s_did_sql_list_cate.mkString(" union all "))
//        .join(df_b_did_cate, Seq("cid", "did"), "outer")
//        .union(df_all_cate)
//        .write.mode(SaveMode.Append)
//        .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.%s".format(table_name), prop)
//    }

    //统计did-cid对应的进出口
    for (i <- 0 to 3) {
      val date = dates(i)
      var b_did_sql_list_product = List[String]()
      var s_did_sql_list_product = List[String]()
      for(b_did <- b_did_list) {
        b_did_sql_list_product = b_did_sql_list_product ++ List("select cid, pid, %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count from df_statistics where month > %s group by cid, pid, %s ".format(b_did, date, b_did))
        b_did_sql_list_product = b_did_sql_list_product ++ List("select 0 as cid, 0 as pid, %s as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count from df_statistics where month > %s group by %s ".format(b_did, date, b_did))
      }
      b_did_sql_list_product = b_did_sql_list_product ++ List("select cid, pid, 0 as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count from df_statistics where month > %s group by pid, cid".format(date))
      val df_b_did_product = spark.sql(b_did_sql_list_product.mkString(" union all "))

      for(s_did <- s_did_list) {
        s_did_sql_list_product = s_did_sql_list_product ++ List("select cid, pid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume, count(distinct suppliers_id) as supp_count from df_statistics where month > %s group by cid, pid, %s ".format(s_did, date, s_did))
        s_did_sql_list_product = s_did_sql_list_product ++ List("select 0 as cid, 0 as pid, %s as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume, count(distinct suppliers_id) as supp_count from df_statistics where month > %s group by %s ".format(s_did, date, s_did))
      }
      s_did_sql_list_product = s_did_sql_list_product ++ List("select cid, pid, 0 as did, sum(total_weight) as exp_total_weight, sum(total_volume) as exp_total_volume, count(distinct suppliers_id) as supp_count from df_statistics where month > %s group by pid, cid".format(date))

      val df_all_product = spark.sql("select 0 as cid, 0 as pid, 0 as did, sum(total_weight) as imp_total_weight, sum(total_volume) as imp_total_volume, count(distinct businesses_id) as busi_count, count(distinct suppliers_id) as supp_count from df_statistics")
        .withColumn("exp_total_weight", col("imp_total_weight"))
        .withColumn("exp_total_volume", col("imp_total_volume"))

      var table_name = "statis_dis_product_" + i
      if (i == 3) {
        table_name = "statis_dis_product"
      }

      spark.sql(s_did_sql_list_product.mkString(" union all "))
        .join(df_b_did_product, Seq("cid", "pid",  "did"), "outer")
        .union(df_all_product)
        .write.mode(SaveMode.Append)
        .jdbc("jdbc:mysql://node118:3306/usa_2018", "usa_2018.%s".format(table_name), prop)
    }

  }

  def get_date(): List[Int] = {
    val format = new SimpleDateFormat("yyyyMM")

    val calendar = Calendar.getInstance()
    val now = new Date()

    //过去半年
    calendar.setTime(now)
    calendar.add(Calendar.MONTH, -6)
    val half_year = calendar.getTime()

    //过去一年
    calendar.setTime(now)
    calendar.add(Calendar.YEAR, -1)
    val one_year = calendar.getTime()

    //过去三年
    calendar.setTime(now)
    calendar.add(Calendar.YEAR, -3)
    val three_year = calendar.getTime()

    List(strToInt(format.format(half_year)), strToInt(format.format(one_year)), strToInt(format.format(three_year)), 0)
  }

  def strToInt(str: String): Int = {
    val regex = """([0-9]+)""".r
    val res = str match{
      case regex(num) => num
      case _ => "0"
    }
    val resInt = Integer.parseInt(res)
    resInt
  }

}
