package com.futhead.scala.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object SparkOperateHBase {
  def main(args: Array[String]): Unit = {

    val hbaseConf= HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "node116:2181,node117:2181,node118:2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "student")

    val sparkConf = new SparkConf()
      .setAppName("SparkSQL")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val stuRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
      classOf[Result])
    val count = stuRDD.count()

    println("Students RDD Count:" + count)
    stuRDD.cache()

    //遍历输出
    stuRDD.foreach({
      case (_, result) =>
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue("info".getBytes(), "name".getBytes()))
        val gender = Bytes.toString(result.getValue("info".getBytes(), "gender".getBytes()))
        val age = Bytes.toString(result.getValue("info".getBytes(), "age".getBytes()))
        println("Row key:" + key + " Name:" + name + " Age:" + age)
    })
  }
}
