package com.spark.sql;

import com.spark.files.csv.Movie;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;

public class SparkDataFrameDemo {
    public static void main(String[] args) {

        SparkConf sc = new SparkConf().setMaster("local").setAppName("SparkDataFrameDemo");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        SQLContext sqlc = new SQLContext(jsc);

        JavaRDD<Movie> moviesRdd = jsc.parallelize(Arrays.asList(
                new Movie(1, "Guardians of the Galaxy", "Action|Adventure|Sci-Fi"),
                new Movie(2, "Prometheus", "Adventure|Mystery|Sci-Fi"),
                new Movie(3, "Split", "Horror|Thriller"),
                new Movie(4, "Sing", "Animation|Comedy|Family"),
                new Movie(5, "Suicide Squad", "Action|Adventure|Fantasy")
        ));

        Dataset<Row> movieDF = sqlc.createDataFrame(moviesRdd, Movie.class);

        System.out.println("Movies DataFrame Schema::");
        movieDF.printSchema();

        System.out.println("Movies DataFrame::");
        movieDF.show();

        //here index 0 is taken as printSchema displays it at 0th index
        Dataset<Row> advMovies = movieDF.filter((FilterFunction<Row>)row -> row.getString(0).contains("Adventure"));
        System.out.println("Filtered DataFrame::");
        advMovies.show();

        jsc.close();
        System.out.println("Done!!!");
    }

}
