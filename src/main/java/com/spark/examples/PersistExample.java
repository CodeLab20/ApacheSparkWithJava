package com.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class PersistExample {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf()
                            .setMaster("local")
                            .setAppName("Persist")
                            .set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<Integer> intList = Arrays.asList(1,2,3,4,5,6);

        JavaRDD<Integer> intRdd = jsc.parallelize(intList, 3).cache();

        JavaRDD<Integer> evenRdd = intRdd.filter(x -> x % 2 == 0);

        JavaRDD<Integer> persistRdd = evenRdd.persist(StorageLevel.MEMORY_AND_DISK());

        System.out.println("Persisted RDD::" + persistRdd.collect());

        persistRdd.unpersist();
        evenRdd.unpersist();
        intRdd.unpersist();

        jsc.close();
        System.out.println("Done!!!");
    }
}
