package com.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLDemo {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                            .master("local")
                            .appName("SparkSQLDemo")
                            .config("spark.sql.warehouse.dir", "c:/temp")
                            .getOrCreate();

        Dataset<Row> csvDs = ss.read().format("csv").option("header", "true").load("InputFiles/Movie_Genres.csv");

        csvDs.createOrReplaceTempView("Test");
        Dataset<Row> moviesDs = ss.sql("select * from Test");

//        System.out.println("SQL Result:::" + moviesDs.collect());

        moviesDs.show();

        ss.close();

        System.out.println("Done!!!");
    }

}
