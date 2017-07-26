package com.futhead.java.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by futhead on 17-7-24.
 */
public class SparkHiveExample {

    public static void main(String[] args) {

        String warehouseLocation = "/home/futhead/app/spark-2.0.2-bin-hadoop2.6/warehouse";

        SparkSession spark = SparkSession.builder()
                .appName("SparkHiveExample")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("create table if not exists src (key int, value string)");
        spark.sql("load data local inpath '/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/kv1.txt' into table src");

        spark.sql("select * from src").show();

        spark.sql("select count(*) from src").show();

        Dataset<Row> sqlDF = spark.sql("select key, value from src where key < 10 order by key");

        Dataset<String> stringDS = sqlDF.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Key: " + row.get(0) + ", Value: " + row.get(1);
            }
        }, Encoders.STRING());
        stringDS.show();

        List<Record> records  = new ArrayList<Record>();
        for (int key = 1; key < 100; key++) {
            Record record = new Record(key, "value_" + key);
            records.add(record);
        }

        Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
        recordsDF.createOrReplaceTempView("records");

        spark.sql("select * from records r join src s on r.key = s.key").show();

        spark.stop();
    }

    public static class Record implements Serializable {

        private int key;
        private String value;

        public Record(int key, String value) {
            this.key = key;
            this.value = value;
        }

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
