package com.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Actions2 {
    public static void main(String[] args) {

        SparkConf sc = new SparkConf()
                            .setMaster("local")
                            .setAppName("Actions-2")
                            .set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<Integer> intList = Arrays.asList(1,2,3,4,5,6);

        JavaRDD<Integer> intRdd = jsc.parallelize(intList);

        //Take Sample
        System.out.println("Take Sample(true, 4) ::" + intRdd.takeSample(true, 4));
        System.out.println("Take Sample(false, 4) ::" + intRdd.takeSample(false, 4));
        System.out.println("Take Sample(true, 4, 9) ::" + intRdd.takeSample(true, 4, 9));

        //Top 2 elements
        System.out.println("Top 2 elements::" + intRdd.top(2, Comparator.reverseOrder()));

        // Fold Action
        Integer fold = intRdd.fold(0, (a, b) -> a + b);
        System.out.println("Fold Value is::" + fold);

        //Reduce Action
        Integer reduce = intRdd.reduce((integer, integer2) -> integer + integer2);
        System.out.println("Reduce value is:" + reduce);

        //for each
        intRdd.foreach(x -> System.out.println("Value:::" + x));

        //save as text file
        intRdd.saveAsTextFile("TextFileDir");

        //save as Object File
        intRdd.saveAsObjectFile("ObjectFileDir");

        jsc.close();
    }
}
