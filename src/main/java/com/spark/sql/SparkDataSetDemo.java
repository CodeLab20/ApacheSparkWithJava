package com.spark.sql;

import com.spark.files.csv.Movie;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

public class SparkDataSetDemo {
    public static void main(String[] args) {
        SparkSession ss =new SparkSession.Builder().master("local").appName("SparkDataSetDemo").getOrCreate();
        JavaSparkContext jsc  = new JavaSparkContext(ss.sparkContext());

        JavaRDD<String> rdd = jsc.textFile("InputFiles/Movie_Genres.csv");
        JavaRDD<String> hdrlessRdd = rdd.filter(s -> !s.startsWith("Rank")); //remove header
        JavaRDD<Movie> mvRdd = hdrlessRdd.map(s -> {
            String[] split = s.split(",");
            return new Movie(Integer.parseInt(split[0]), split[1], split[2]);
        });

        System.out.println("Movies created using Bean::");

        //here RDD<Movie> is required. so each csv row is mapped to Movie
        Dataset<Movie> mvDS = ss.createDataset(mvRdd.rdd(), Encoders.bean(Movie.class));
        Dataset<Movie> adventure = mvDS.filter((FilterFunction<Movie>) mv -> mv.getGenres().contains("Adventure"));
        adventure.show();

        System.out.println("Movies where rand > 100");
        mvDS.filter("rank > 100").show();

        System.out.println("Movies where genre = Horror using col function::");
        mvDS.filter(col("genres").like("%Horror%")).show();


        //using RowFactory to create rows and creating custom schema
        JavaRDD<Row> mvRows = hdrlessRdd.map(s -> {
            String[] split = s.split(",");
            return RowFactory.create(Integer.parseInt(split[0]), split[1], split[2]);
        });

        //During custom schema creation, column names are changed.
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("ID", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("MovieName", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("MovieGenres", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

        System.out.println("Movies created using Custom schema:::");

        Dataset<Row> mvDf = ss.createDataFrame(mvRows, schema);
        mvDf.printSchema();
        mvDf.show();

        jsc.close();
        System.out.println("Done!!!");
    }

}
