package com.futhead.java.streaming;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created by futhead on 17-7-26.
 */
public class QueueStream {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Queue<JavaRDD<Integer>> rddQueue = new LinkedList<JavaRDD<Integer>>();

        List<Integer> list = Lists.newArrayList();
        for (int i = 0; i < 30; i++) {
            list.add(i);
        }

        for (int i = 0; i < 30; i++) {
            rddQueue.add(jssc.sparkContext().parallelize(list));
        }

        JavaDStream<Integer> inputStream = jssc.queueStream(rddQueue);

        JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer % 10, 1);
            }
        });

        JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reducedStream.print();
        jssc.start();
        jssc.awaitTermination();

    }
}
