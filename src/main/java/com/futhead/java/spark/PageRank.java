package com.futhead.java.spark;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by futhead on 17-7-16.
 */
public class PageRank {

    private static final Pattern SPACES = Pattern.compile(" ");

    static void showWarning() {
        String warning = "WARN: This is a naive implementation of PageRank " +
                "and is given as an example! \n" +
                "Please use the PageRank implementation found in " +
                "org.apache.spark.graphx.lib.PageRank for more conventional use.";
        System.err.println(warning);
    }

    private static class Sum implements Function2<Double, Double, Double> {

        public Double call(Double v1, Double v2) throws Exception {
            return v1 + v2;
        }
    }

    public static void main(String[] args) {
        showWarning();

        SparkSession spark = SparkSession.builder().master("local").appName("PageRank").getOrCreate();

        JavaRDD<String> lines = spark.read().textFile("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/data/mllib/pagerank_data.txt").javaRDD();

        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] parts = SPACES.split(s);
                return new Tuple2<String, String>(parts[0], parts[1]);
            }
        }).distinct().groupByKey().cache();

        JavaPairRDD<String, Double> ranks = links.mapValues(new Function<Iterable<String>, Double>() {
            public Double call(Iterable<String> v1) throws Exception {
                return 1.0;
            }
        });

        //Calculates URL contributions to the rank of other URLs
        JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
            public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> iterableDoubleTuple2) throws Exception {
                int urlCount = Iterables.size(iterableDoubleTuple2._1);
                List<Tuple2<String, Double>> results = new ArrayList<Tuple2<String, Double>>();
                for (String n : iterableDoubleTuple2._1) {
                    results.add(new Tuple2<String, Double>(n, iterableDoubleTuple2._2 / urlCount));
                }
                return results.iterator();
            }
        });

        ranks = contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
            public Double call(Double sum) throws Exception {
                return 0.15 + sum * 0.85;
            }
        });

        List<Tuple2<String, Double>> output = ranks.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + "has rank: " + tuple._2() + ".");
        }

        spark.stop();
    }
}
