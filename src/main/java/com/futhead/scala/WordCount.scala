import java.io.File

import scala.io.Source

/**
  * scala 版本的wordcount
  * Created by futhead on 2018/5/20.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val dirfile = new File("D:\\program\\spark-2.1.1-bin-hadoop2.7\\conf");
    val files = dirfile.listFiles()
    for(file <- files)
      println(file)
    val wordsMap = scala.collection.mutable.Map[String, Int]()
    files.foreach(
      file => Source.fromFile(file).getLines().foreach(
        line => line.split(" ").foreach(
          word => {
            if (wordsMap.contains(word)) {
              wordsMap(word) += 1
            } else {
              wordsMap += (word -> 1)
            }
          }
        )
      )
    )
    for ((key, value) <- wordsMap) {
      println(key + ": " + value)
    }
  }
}