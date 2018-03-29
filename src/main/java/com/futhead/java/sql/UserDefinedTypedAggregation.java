package com.futhead.java.sql;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

/**
 * Created by futhead on 17-7-20.
 */
public class UserDefinedTypedAggregation {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("UserDefinedTypedAggregation").master("local").getOrCreate();
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);

        String path = "/home/futhead/app/spark-2.0.2-bin-hadoop2.6/examples/src/main/resources/employees.json";
        Dataset<Employee> ds = spark.read().json(path).as(employeeEncoder);

        ds.show();

        MyAverage myAverage = new MyAverage();

        TypedColumn<Employee, Double> averageSalary = myAverage.toColumn().name("average_salary");
        Dataset<Double> result = ds.select(averageSalary);
        result.show();

        spark.stop();

    }

    public static class MyAverage extends Aggregator<Employee, Average, Double> {

        public Average zero() {
            return new Average(0L, 0L);
        }

        public Average reduce(Average buffer, Employee employee) {
            long newSum = buffer.getSum() + employee.getSalary();
            long newCount = buffer.getCount() + 1;
            buffer.setSum(newSum);
            buffer.setCount(newCount);
            return buffer;
        }

        public Average merge(Average b1, Average b2) {
            b1.setSum(b1.getSum() + b2.getSum());
            b1.setCount(b1.getCount() + b2.getCount());
            return b1;
        }

        public Double finish(Average reduction) {
            return Double.valueOf(reduction.getSum()) / reduction.getCount();
        }

        public Encoder<Average> bufferEncoder() {
            return Encoders.bean(Average.class);
        }

        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }

    public static class Employee implements Serializable {
        private String name;
        private long salary;

        // Constructors, getters, setters...
        // $example off:typed_custom_aggregation$
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
        // $example on:typed_custom_aggregation$
    }

    public static class Average implements Serializable  {
        private long sum;
        private long count;

        // Constructors, getters, setters...
        // $example off:typed_custom_aggregation$
        public Average() {
        }

        public Average(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
        // $example on:typed_custom_aggregation$
    }
}
