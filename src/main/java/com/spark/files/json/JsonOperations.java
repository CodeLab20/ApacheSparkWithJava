package com.spark.files.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JsonOperations {

    private static final String CCHolders = "InputFiles/ccholders.json";

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                            .master("local")
                            .appName("JSON_Operations")
                            .config("spark.hadoop.validateOutputSpecs", "false")
                            .config("spark.sql.warehouse.dir", "C:\\temp")
                            .getOrCreate();

        //Reading JSON file as a text and creating object by self
        System.out.println("Reading JSON as Text File");

        ObjectMapper mp = new ObjectMapper();

        JavaRDD<CCHolder> ccHoldersRDD = ss.read().textFile(CCHolders)
                                            .toJavaRDD().map(line -> mp.readValue(line, CCHolder.class));

        ccHoldersRDD.take(10).stream().forEach(c-> System.out.println(c));


        //Reading json file as json
        System.out.println("Reading JSON in json format");
        Dataset<Row> cchJson = ss.read().json(CCHolders);

        cchJson.printSchema();
        cchJson.show();

        //Reading CSV file providing custom schema
        System.out.println("Reading JSON with custom schema.");
        StructType schema = new StructType(new StructField[]{

                new StructField("name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("email", DataTypes.StringType, true, Metadata.empty()),
                new StructField("city", DataTypes.StringType, true, Metadata.empty()),
                new StructField("mac", DataTypes.StringType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("creditcard", DataTypes.StringType, true, Metadata.empty())
        });


        Dataset<Row> csJson = ss.read().format("json")
                                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss Z")
                                .schema(schema)
                                .json(CCHolders );

        csJson.printSchema();
        csJson.show();


        System.out.println("Writing JSON to disk");
        csJson.write().format("json").save("CCHolders");

        ss.close();

        System.out.println("Done!!!");
    }
}
