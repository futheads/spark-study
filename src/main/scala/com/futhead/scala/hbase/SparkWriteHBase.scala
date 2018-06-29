package com.futhead.scala.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

object SparkWriteHBase {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val tableName = "student"
    val conf = sc.hadoopConfiguration
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    conf.set("hbase.zookeeper.quorum", "node116:2181,node117:2181,node118:2181")

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26","4,Guanhua,M,27"))
    val rdd = indataRDD.map(line => line.split(",")).map{arr =>
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("gender"),Bytes.toBytes(arr(2)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(arr(3).toInt))
      (new ImmutableBytesWritable(), put)
    }
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
