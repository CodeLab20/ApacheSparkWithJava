package com.spark.shared;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Map;

public class AccumulatorDemo {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "BroadCastVar");

        //Long Accumulator
        LongAccumulator longAccumulator = jsc.sc().longAccumulator("ErrorCounter");

        JavaRDD<String> excRdd = jsc.textFile("InputFiles/Exceptions.txt");
        excRdd.foreach(line ->{
            if(line.contains("Error"))
            {
                longAccumulator.add(1);
                System.out.println("Line with Error::" + line);
            }
        });

        System.out.println("Error found for " + longAccumulator.value() + " times.");

        //Collection Accumulator
        CollectionAccumulator<Object> collectionAccumulator = jsc.sc().collectionAccumulator("CollectionAccumulator");

        excRdd.foreach(line ->{
            if(line.contains("Error"))
            {
                collectionAccumulator.add("1");
                System.out.println("Collection Line with Error::" + line);
            }
        });

        System.out.println("Collection Error found for " + collectionAccumulator.value() + " times.");

        //Custom Accumulator
        ListAccumulator listAccumulator = new ListAccumulator();
        jsc.sc().register(listAccumulator, "CustomAccumulator");

        excRdd.foreach(line ->{
            if(line.contains("Error"))
            {
                listAccumulator.add("1");
                System.out.println("Custom Line with Error::" + line);
            }
        });

        System.out.println("List Accumulator Error found for " + listAccumulator.value() + " times.");

        jsc.close();
        System.out.println("Done!!!");
    }
}
