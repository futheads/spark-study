package com.futhead.java.sql;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by futhead on 17-7-24.
 */
public class SparkHiveExample {

    public static void main(String[] args) {

        String warehouseLocation = "/home/futhead/app/spark-2.0.2-bin-hadoop2.6";

        SparkSession spark = SparkSession.builder()
                .appName("SparkHiveExample")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("create table if not exists src (key int, value string)");
        spark.sql("load data local inpath '/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/kv1.txt' into table src");

        spark.sql("select * from src").show();
    }

    public static class Record implements Serializable {
        private int key;
        private String value;

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

}
