package com.futhead.java.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by futhead on 17-7-16.
 */
public class SparkPi {

    public static void main(String[] args) {

//        SparkSession spark = SparkSession.builder().master("local[*]").appName("SparkPi").getOrCreate();
        
        SparkSession spark = SparkSession.builder().appName("SparkPi").getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        int slices = 2;
        int n = 100000 * slices;
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < n; i++) {
            list.add(i);
        }

        JavaRDD<Integer> dataSet = jsc.parallelize(list, 2);

        int count = dataSet.map(new Function<Integer, Integer>() {
            public Integer call(Integer v1) throws Exception {
                double x = Math.random() * 2 - 1;
                double y = Math.random() * 2 - 1;
                return (x * x + y * y < 1) ? 1 : 0;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("Pi is roughly " + 4.0 * count / n);

        spark.stop();
    }
}
