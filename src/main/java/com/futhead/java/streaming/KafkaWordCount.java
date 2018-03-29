package com.futhead.java.streaming;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Map;

/**
 * Created by futhead on 17-7-26.
 */
public class KafkaWordCount {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, new Duration(2000));

        Map<String, Integer> topicMap = new HashedMap();
        topicMap.put("topicA", 1);

    }

}
