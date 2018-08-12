package com.futhead.scala.mllib

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Administrator on 2018/5/20.
  */
object MLPipelines {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("MLPipelines").getOrCreate()
//    val training = spark.createDataFrame(
//      Seq(("a b c d e spark", 1.0), ("b d", 2.0), ("spark f g h", 3.0), ("hadoop mapreduce", 4.0)))
//      .toDF("text", "label")
//    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
//    val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
//    val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
//    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
//    val model = pipeline.fit(training)

    // Now we can optionally save the fitted pipeline to disk
//    model.write.overwrite().save("spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
//    pipeline.write.overwrite().save("/tmp/unfit-lr-model")

    // And load it back in during production
    val sameModel = PipelineModel.load("spark-logistic-regression-model")

    val test = spark.createDataFrame(Seq((4L, "spark i j k"),(5L, "l m n"),(6L, "spark a"),(7L, "apache hadoop")))
      .toDF("id", "text")
    sameModel.transform(test).show()

  }
}
