package com.futhead.scala.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/20.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val inputFile = "file:///D:\\program\\spark-2.1.1-bin-hadoop2.7\\README.md"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}
