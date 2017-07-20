package com.futhead.java.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by futhead on 17-7-18.
 */
public class SQLDataSourceExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("SQLDataSourceExample").getOrCreate();

        runBasicDataSourceExample(spark);
        rubBasicParquetExample(spark);
        runParquetSchemaMergingExample(spark);
        runJdbcDatasetExample(spark);

        spark.stop();
    }

    public static void runBasicDataSourceExample(SparkSession spark) {
        Dataset<Row> userDF = spark.read().load("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/users.parquet");
        userDF.select("name", "favorite_color").write().save("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/namesAndFavorColors.parquet");

        Dataset<Row> peopleDF = spark.read().format("json").json("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.json");
        peopleDF.select("name", "age").write().format("parquet").save("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/namesAndages.parquet");

        Dataset<Row> sqlDF = spark.sql("select * from parquet.`/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/users.parquet`");
    }

    public static void rubBasicParquetExample(SparkSession spark) {
        Dataset<Row> parquetFileDF = spark.read().parquet("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/namesAndages.parquet");
        parquetFileDF.createOrReplaceTempView("parquetFile");
        Dataset<Row> namesDF = spark.sql("select name from parquetFile where age between 13 and 19");
        Dataset<String> namesDS = namesDF.map(new MapFunction<Row, String>() {
            public String call(Row row) throws Exception {
                return "Name: " + row.get(0);
            }
        }, Encoders.STRING());
        namesDS.show();
    }

    public static void runParquetSchemaMergingExample(SparkSession spark) {
        List<Square> squares = new ArrayList<Square>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/test_table/key=1");

        List<Cube> cubes = new ArrayList<Cube>();
        for (int value = 6; value < 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/test_table/key=2");

        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/test_table");
        mergedDF.printSchema();
    }

    private static void runJdbcDatasetExample(SparkSession spark) {

        Dataset<Row> peopleDF = spark.read().format("json").json("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/people.json");
        peopleDF.write().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306")
                .option("dbtable", "spark.people")
                .option("user", "futhead")
                .option("password", "futhead")
                .save();

        Dataset<Row> jdbcDF = spark.read().format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306")
                .option("dbtable", "spark.people")
                .option("user", "futhead")
                .option("password", "futhead")
                .load();
        jdbcDF.show();
    }

    public static class Square implements Serializable {
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
