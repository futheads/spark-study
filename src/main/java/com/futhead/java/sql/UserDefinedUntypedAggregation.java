package com.futhead.java.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by futhead on 17-7-21.
 */
public class UserDefinedUntypedAggregation {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("UserDefinedUntypedAggregation").master("local").getOrCreate();

        spark.udf().register("MyVerage", new MyAverage());

        Dataset<Row> df = spark.read().json("/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/employees.json");

        df.createOrReplaceTempView("employees");;
        df.show();

        Dataset<Row> result = spark.sql("select myAverage(salary) as average_salary from employees");

        spark.stop();
    }

    public static class MyAverage extends UserDefinedAggregateFunction {

        private StructType inputSchema;
        private StructType bufferSchema;

        public MyAverage() {
            List<StructField> inputFields = new ArrayList<StructField>();
            inputFields.add(DataTypes.createStructField("inputColumn", DataTypes.LongType, true));
            inputSchema = DataTypes.createStructType(inputFields);

            List<StructField> bufferFields = new ArrayList<StructField>();
            bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
            bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
            bufferSchema = DataTypes.createStructType(bufferFields);
        }

        public StructType inputSchema() {
            return null;
        }

        public StructType bufferSchema() {
            return null;
        }

        public DataType dataType() {
            return null;
        }

        public boolean deterministic() {
            return false;
        }

        public void initialize(MutableAggregationBuffer buffer) {

        }

        public void update(MutableAggregationBuffer buffer, Row input) {

        }

        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

        }

        public Object evaluate(Row buffer) {
            return null;
        }
    }
}
