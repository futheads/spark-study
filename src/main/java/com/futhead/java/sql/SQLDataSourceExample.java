package com.futhead.java.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by futhead on 17-7-18.
 */
public class SQLDataSourceExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("SQLDataSourceExample").getOrCreate();

        runBasicDataSourceExample(spark);

    }

    public static void runBasicDataSourceExample(SparkSession spark) {
        Dataset<Row> userDF = spark.read().load("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/users.parquet");
    }

    public class Square implements Serializable {
        private int value;
        private int square;

        private void getHello() {
            System.out.println("Hello");
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void setSquare(int square) {
            this.square = square;
        }

        public int getValue() {
            return value;
        }

        public int getSquare() {
            return square;
        }
    }

    public static class Cube implements Serializable {
        private int value;
        private int cube;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getCube() {
            return cube;
        }

        public void setCube(int cube) {
            this.cube = cube;
        }
    }
}
