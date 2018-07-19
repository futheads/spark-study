package com.futhead.scala.hbase

import com.futhead.scala.es.Elasticsearch.Trip
import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.commons.codec.digest.DigestUtils
import org.elasticsearch.spark.rdd.EsSpark

object Import {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("import2es").setMaster("local[*]").set("es.nodes", "node118:9200")
    conf.set("es.index.auto.create", "true")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("file:///d:/2015-03-01.log")
    val data = rdd.map(_.split("@")).map{x=>(x(0)+x(1),x(2))}
    val result = data.foreachPartition{x => {
      val conf= HBaseConfiguration.create();
      conf.set(TableInputFormat.INPUT_TABLE,"data");
      conf.set("hbase.zookeeper.quorum", "node116:2181,node117:2181,node118:2181")
      val table = new HTable(conf,"data");table.setAutoFlush(false,false);
      table.setWriteBufferSize(3*1024*1024); x.foreach{y => {
        var put= new Put(Bytes.toBytes(y._1));
        put.add(Bytes.toBytes("v"),Bytes.toBytes("value"),Bytes.toBytes(y._2));
        table.put(put)};
        table.flushCommits}}
    }

    sc.stop()

  }
}
