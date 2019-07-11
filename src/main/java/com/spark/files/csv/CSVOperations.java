package com.spark.files.csv;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;


public class CSVOperations {

    public static final String MOVIES = "InputFiles/Movie_Genres.csv";

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                            .master("local").appName("CSVOperations")
                            .config("spark.hadoop.validateOutputSpecs", "false")
                            .getOrCreate();

        //Reading CSV file as a text and parsing line by self
        System.out.println("Reading CSV as Text File");

        JavaRDD<Movie> movieRDD = ss.read().textFile(MOVIES)
                                    .javaRDD()
                                    .filter(line -> ! line.startsWith("Rank"))  //This is done to skip header
                                    .map(line -> Movie.parseMovie(line));

        movieRDD.take(10).stream().forEach(m -> System.out.println(m));


        //Reading csv file as csv
        System.out.println("Reading CSV in csv format");
        Dataset<Row> movies = ss.read().format("csv").option("header", "true").option("inferSchema", "true").csv(MOVIES);

        movies.printSchema();
        movies.show();

        //Reading CSV file providing custom schema
        System.out.println("Reading CSV with custom schema.");
        StructType schema = new StructType(new StructField[]{
           new StructField("movieID", DataTypes.IntegerType, true, Metadata.empty()),
           new StructField("movieName", DataTypes.StringType, true, Metadata.empty()),
           new StructField("movieGenres", DataTypes.StringType, true, Metadata.empty())
        });

        Dataset<Row> mcsv = ss.read().format("csv").option("header", "true").schema(schema).csv(MOVIES);

        mcsv.printSchema();
        mcsv.show();

        //Writing CSV to disk with compression
        System.out.println("Writing CSV to disk");

        mcsv.write().format("csv")
                .option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec")
                .save("Movies");

        ss.close();
        System.out.println("Done!!!");
    }
}
