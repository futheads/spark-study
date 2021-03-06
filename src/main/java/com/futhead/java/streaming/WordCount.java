package com.futhead.java.streaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCount {

	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws InterruptedException {
		
//		args[0] = "localhost";
//		args[1] = "";

	    // Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

	    // Create a JavaReceiverInputDStream on target ip:port and count the
	    // words in input stream of \n delimited text (eg. generated by 'nc')
	    // Note that no duplication in storage level only for running locally.
	    // Replication necessary in distributed scenario for fault tolerance.
	    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
	            "study01", Integer.parseInt("9999"), StorageLevels.MEMORY_AND_DISK_SER);
	    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
	      @Override
	      public Iterator<String> call(String x) {
	    	  System.out.println(x);
	        return Arrays.asList(SPACE.split(x)).iterator();
	      }
	    });
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	      new PairFunction<String, String, Integer>() {
	        @Override
	        public Tuple2<String, Integer> call(String s) {
	          return new Tuple2<>(s, 1);
	        }
	      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
	        @Override
	        public Integer call(Integer i1, Integer i2) {
	          return i1 + i2;
	        }
	      });

	    wordCounts.print();
	    ssc.start();
	    ssc.awaitTermination();
	}
}
