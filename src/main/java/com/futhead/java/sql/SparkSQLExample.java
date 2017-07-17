package com.futhead.java.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by futhead on 17-7-17.
 */
public class SparkSQLExample {

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder().appName("SparkSQLExample").master("local").getOrCreate();

//        runBasicDataFrameExample(spark);
        runDatasetCreationExample(spark);

        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {

        Dataset<Row> df = spark.read().json("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.json");

        // Displays the content of the DataFrame to stdout
        df.show();

        // Print the schema in a tree format
        df.printSchema();

        df.select("name").show();

        df.select(functions.col("name"), functions.col("age").plus(1)).show();

        df.filter(functions.col("age").gt(21)).show();

        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();

        df.createGlobalTempView("people");
        spark.sql("select * from global_temp.people").show();
        spark.newSession().sql("select * from global_temp.people").show();

    }

    private static void runDatasetCreationExample(SparkSession spark) {
        Person person = new Person();
        person.setName("futhead");
        person.setAge(27);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDB = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDB = primitiveDB.map(new MapFunction<Integer, Integer>() {
            public Integer call(Integer value) throws Exception {
                return value + 1;
            }
        }, integerEncoder);
        for (Integer i : (Integer[]) transformedDB.collect()) {
            System.out.print(i + ", ");
        }
        System.out.println();

        String path = "/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();

    }
}
