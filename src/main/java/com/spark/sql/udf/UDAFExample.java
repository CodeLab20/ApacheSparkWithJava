package com.spark.sql.udf;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class UDAFExample {
    public static void main(String[] args) {
        SparkSession ss =new SparkSession.Builder()
                .master("local[1]").appName("UDAFExample")
                .getOrCreate();



        System.out.println("Read Employee Data:::");
        Dataset<Row> empDs = ss.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("InputFiles/employees.csv");

        empDs.printSchema();

        //Register User defined aggregate function
        AverageUDAF avgUdaf = new AverageUDAF();
        ss.udf().register("AvgUDAF", avgUdaf);

        empDs.createOrReplaceTempView("employees");

        System.out.println("Department wise average salary is::");
        ss.sql("select deptno, AvgUDAF(sal) as UdafAvgSal from employees group by deptno").show();

        //Register User defined aggregate Typesafe function
        TypeSafeAverageUDAF tsAvgUdf = new TypeSafeAverageUDAF();

        Dataset<Employee> emps = empDs.as(Encoders.bean(Employee.class));
        emps.printSchema();

        TypedColumn<Employee, Double> udfCol = tsAvgUdf.toColumn().name("TypeSafeUDFAvg");

        System.out.println("Company Average Salary:::");
        emps.select(udfCol).show();

        System.out.println("Done!!!");

        ss.close();

    }
}
