package com.futhead.scala.mllib

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/20.
  */
object TfIdf {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("TF-IDF").getOrCreate()
    val sentenceData = spark.createDataFrame(Seq((0, "I heard about Spark and I love Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
//    wordsData.show(false)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.select("rawFeatures").show(false)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(3).foreach(println)
  }
}
