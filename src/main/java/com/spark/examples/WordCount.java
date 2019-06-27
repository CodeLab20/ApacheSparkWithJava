package com.spark.examples;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        System.out.println(System.getProperty("hadoop.home.dir"));
        String inputPath = args[0];
        String outputPath = args[1];

        FileUtils.deleteQuietly(new File(outputPath));

        System.out.println("Input::" + inputPath);
        System.out.println("Output::" + outputPath);

        JavaSparkContext jsc = new JavaSparkContext("local", "WordCount");
        JavaRDD<String> rdd = jsc.textFile(inputPath);

        JavaPairRDD<String, Integer> resrdd = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((w1, w2) -> w1 + w2);

        resrdd.saveAsTextFile(outputPath);
        System.out.println("Done!!!");
    }

}
