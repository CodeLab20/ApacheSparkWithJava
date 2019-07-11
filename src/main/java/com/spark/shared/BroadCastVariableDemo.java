package com.spark.shared;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Map;

public class BroadCastVariableDemo {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "BroadCastVar");

        JavaPairRDD<String, String> personCityRdd = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<>("1", "101"), new Tuple2<>("2", "102"),
                new Tuple2<>("3", "107"), new Tuple2<>("4", "103"),
                new Tuple2<>("11", "101"), new Tuple2<>("12", "102"),
                new Tuple2<>("13", "107"), new Tuple2<>("14", "103")
        ));

        JavaPairRDD<String, String> cityCountryRdd = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<>("101", "INDIA"), new Tuple2<>( "102", "UK"),
                new Tuple2<>("107", "USA"), new Tuple2<>( "103", "GERMANY")
        ));

        Broadcast<Map<String, String>> broadcast = jsc.broadcast(cityCountryRdd.collectAsMap());

        JavaRDD<Tuple3<String, String, String>> resRdd = personCityRdd.map(tpl -> new Tuple3<>(tpl._1(), tpl._2(), broadcast.value().get(tpl._2())));

        System.out.println("Person, City, Country Data::" + resRdd.collect());

        broadcast.unpersist();
        broadcast.destroy();

        jsc.close();
        System.out.println("Done!!!");
    }
}
