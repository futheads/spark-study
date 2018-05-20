package com.futhead.scala.mllib

import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/20.
  */
object Word2Vector {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("Word2Vec").getOrCreate()
    val documentDf = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)
    val model = word2Vec.fit(documentDf)
    val result = model.transform(documentDf)
    result.select("result").take(3).foreach(println)
  }
}
