package com.spark.files.xml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class XMLOperations {

    private static final String LOTTERY = "InputFiles/Lottery.xml";

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                            .master("local")
                            .appName("XML_Operations")
                            .config("spark.hadoop.validateOutputSpecs", "false")
                            .config("spark.sql.warehouse.dir", "C:\\temp")
                            .getOrCreate();

        //Reading XML as text file
        System.out.println("Reading XML Data");
        Map<String, String> xmlConfig = new HashMap<>();
        xmlConfig.put("rowTag", "row");
        xmlConfig.put("failFast", "true");

        Dataset<Row> xmlLotteries = ss.read().format("xml").options(xmlConfig).load(LOTTERY);
        xmlLotteries.printSchema();
        xmlLotteries.show();

        //Writing XML
        System.out.println("Writing XML Data");
        xmlLotteries.write().format("xml")
                .option("rootTag", "LotteryWins")
                .option("rowTag", "Lottery")
                .save("Lotteries");

        ss.close();

        System.out.println("Done!!");

    }
}
