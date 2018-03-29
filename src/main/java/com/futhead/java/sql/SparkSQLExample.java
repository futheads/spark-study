package com.futhead.java.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        runBasicDataFrameExample(spark);
        runDatasetCreationExample(spark);
        runInferSchemaExample(spark);
        runProgrammaticSchemaExample(spark);

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

    private static void runInferSchemaExample(SparkSession spark) {
        JavaRDD<Person> peopleRDD = spark.read().textFile("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.txt").javaRDD().map(new Function<String, Person>() {
            public Person call(String v1) throws Exception {
                String [] pars = v1.split(",");
                Person person = new Person();
                person.setName(pars[0]);
                person.setAge(Integer.valueOf(pars[1].trim()));
                return person;
            }
        });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("select name from people where age between 13 and 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(new MapFunction<Row, String>() {
            public String call(Row value) throws Exception {
                return "Name: " + value.getString(0);
            }
        }, stringEncoder);
        teenagerNamesByIndexDF.show();
    }

    public static void runProgrammaticSchemaExample(SparkSession spark) {
        JavaRDD<String> peopleRDD = spark.sparkContext().textFile("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.txt", 1).toJavaRDD();
        String schemaString = "name age";

        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1].trim());
            }
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        peopleDataFrame.createOrReplaceTempView("people");

        Dataset<Row> results = spark.sql("select name from people");

        Dataset<String> namesDS = results.map(new MapFunction<Row, String>() {
            public String call(Row value) throws Exception {
                return "Name: " + value.get(0);
            }
        }, Encoders.STRING());

        namesDS.show();

    }
}
