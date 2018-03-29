package com.futhead.java.streaming;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.futhead.java.dao.CategaryClickCountDAO;
import com.futhead.java.dao.CategarySearchClickCountDAO;
import com.futhead.java.model.CategaryClickCount;
import com.futhead.java.model.CategarySearchClickCount;
import com.futhead.java.model.ClickLog;
import com.futhead.java.util.DateUtil;

import scala.Tuple2;
import scala.Tuple3;

public class StatStreamingApp {

	public static void main(String[] args) throws InterruptedException {

		String brokers = "study01:9092,study02:9092,study03:9092";
		String topics = "flumeTopic";

		SparkConf conf = new SparkConf().setMaster("yarn-cluster").setAppName("yarn-cluster model");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

		// kafka相关参数，必要！缺了会报错
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("bootstrap.servers", brokers);
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

		// 通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
		JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(ssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topicsSet, kafkaParams));
		JavaDStream<String> logs = lines.map(x -> x.value().toString());
		
		JavaDStream<ClickLog> cleanLog = logs.map(new Function<String, ClickLog>() {

			@Override
			public ClickLog call(String s) throws Exception {
              String[] infos = s.split("\t");
              String url = infos[2].split(" ")[1];
              int categrayId = 0;
              if(url.startsWith("/www")) {
              	categrayId = Integer.parseInt(url.split("/")[2]);
              }
              return new ClickLog(infos[0], DateUtil.parseToStr(infos[1]), categrayId, infos[3], Integer.parseInt(infos[4]));
			}
			
		}).filter(new Function<ClickLog, Boolean>() {

			@Override
			public Boolean call(ClickLog log) throws Exception {
				return log.getCategaryId() != 0;
			}
			
		});
		
		cleanLog.print();
		//每个类别的每天的点击量（day_categoryId, 1）
		JavaPairDStream<String, Long> categories = cleanLog.mapToPair(new PairFunction<ClickLog, String, Long>() {

			@Override
			public Tuple2<String, Long> call(ClickLog log) throws Exception {
				return new Tuple2<String, Long>(log.getTime() + "_" + log.getCategaryId(), 1L);
			}
			
		}).reduceByKey(new Function2<Long, Long, Long>() {

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		categories.foreachRDD(rdd -> {
			
			rdd.foreachPartition(partitionOfRecords -> {
				List<CategaryClickCount> list = new ArrayList<>();
			    while (partitionOfRecords.hasNext()) {
			    	Tuple2<String, Long> next = partitionOfRecords.next();
			    	list.add(new CategaryClickCount(next._1, next._2));
			    }
			    CategaryClickCountDAO.save(list);
			  });
        });  
		
		//每个栏目下面从渠道过来的流量20171122_www.baidu.com_1 100 20171122_2（渠道）_1（类别） 100
		//124.30.187.10	2017-11-20 00:39:26	"GET www/6 HTTP/1.0" https:/www.sogou.com/web?qu=我的体育老师	302
		cleanLog.map(new Function<ClickLog, Tuple3<String, String, Integer>>() {

			@Override
			public Tuple3<String, String, Integer> call(ClickLog log) throws Exception {
				String url = log.getRefer().replace("//", "/");
				String[] splits = url.split("/");
				String host = "";
				if(splits.length > 2) {
					host = splits[1];
				}
				return new Tuple3<String, String, Integer>(host, log.getTime(), log.getCategaryId());
			}
		}).filter(new Function<Tuple3<String,String,Integer>, Boolean>() {

			@Override
			public Boolean call(Tuple3<String, String, Integer> tuple3) throws Exception {
				return !tuple3._1().equals("");
			}
			
		}).mapToPair(new PairFunction<Tuple3<String,String,Integer>, String, Long>() {

			@Override
			public Tuple2<String, Long> call(Tuple3<String, String, Integer> x) throws Exception {
				return new Tuple2<String, Long>(x._2() + "_" + x._1() + "_" + x._3(), 1L);
			}
			
		}).reduceByKey(new Function2<Long, Long, Long>() {

			@Override
			public Long call(Long v1, Long v2) throws Exception {
				return v1 + v2;
			}
			
		}).foreachRDD(rdd -> {
			rdd.foreachPartition(partitionOfRecords -> {
				List<CategarySearchClickCount> list = new ArrayList<>();
			    while (partitionOfRecords.hasNext()) {
			    	Tuple2<String, Long> next = partitionOfRecords.next();
			    	list.add(new CategarySearchClickCount(next._1, next._2));
			    }
			    CategarySearchClickCountDAO.save(list);
			  });
        });  
		
		ssc.start();
        ssc.awaitTermination();
        ssc.close();
	}
}
