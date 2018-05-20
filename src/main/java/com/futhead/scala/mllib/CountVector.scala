package com.futhead.scala.mllib

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/20.
  */
object CountVector {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("CountVector").getOrCreate()
    val df = spark.createDataFrame(Seq((0, Array("a", "b", "c")), (1, Array("a", "b", "b", "c", "a"))))
      .toDF("id", "words")
    val cvModel : CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("features")
      .setVocabSize(3).setMinDF(2).fit(df)
//    println(cvModel.vocabulary)
    cvModel.transform(df).show(false)
    val cvm = new CountVectorizerModel(Array("a", "b", "c")).setInputCol("words").setOutputCol("features")
//    cvm.transform(df).select("features").foreach{ println }
  }

}
