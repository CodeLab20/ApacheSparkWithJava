package com.spark.sql.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class UDFExample {
    public static void main(String[] args) {
        SparkSession ss =new SparkSession.Builder()
                .master("local[1]").appName("UDFExample")
                .getOrCreate();

        UDF2 calculateDays = new CalculateDaysUDF();
        ss.udf().register("CalculateDays", calculateDays, DataTypes.LongType);

        System.out.println("Read Employee Data:::");
        Dataset<Row> empDs = ss.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("InputFiles/employees.csv");

        empDs.printSchema();

        empDs.createOrReplaceTempView("employees");

        System.out.println("Employee info with total number of Days");
        ss.sql("select empno, ename, CalculateDays(hiredate, 'dd-MM-yyyy') as TotalDays from employees").show();

        ss.close();

    }
}
