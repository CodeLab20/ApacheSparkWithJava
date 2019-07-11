package com.spark.partition;

import org.apache.spark.HashPartitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Array;
import scala.Tuple2;
import scala.math.Ordering;
import scala.reflect.ClassTag$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PartitionExample {

    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "PartitionExample");

        //Hash Partitioner
        JavaPairRDD<Integer, String> intPrRdd = jsc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"), new Tuple2<>(2, "B"),
                new Tuple2<>(3, "C"), new Tuple2<>(4, "D"), new Tuple2<>(5, "E"), new Tuple2<>(6, "F"),
                new Tuple2<>(7, "G"), new Tuple2<>(8, "H")), 3);

        System.out.println("Before Partition NumPartitions::" + intPrRdd.getNumPartitions());

        JavaPairRDD<Integer, String> repartition = intPrRdd.repartition(4);
        System.out.println("After Re-Partition NumPartitions:::" + repartition.getNumPartitions());

        JavaPairRDD<Integer, String> hashPartedRdd = repartition.partitionBy(new HashPartitioner(2));

        JavaRDD<String> partnData = hashPartedRdd.mapPartitionsWithIndex((prtnIdx, tupleItr) -> {
            List<String> lst = new ArrayList<>();
            while (tupleItr.hasNext())
                lst.add("Partition Num::" + prtnIdx + ", Key::" + tupleItr.next()._1());

            return lst.iterator();
        }, true);

        System.out.println("Hash Partitioned Data::" + partnData.collect());


        //Range Partitioning
        RDD<Tuple2<Integer, String>> rdd = intPrRdd.rdd();
        RangePartitioner rp = new RangePartitioner(4, rdd, true, Ordering.Int$.MODULE$, ClassTag$.MODULE$.apply(Integer.class) );
        JavaPairRDD<Integer, String> rangePartedRdd = intPrRdd.partitionBy(rp);

        JavaRDD<String> rangePartnData = rangePartedRdd.mapPartitionsWithIndex((prtnIdx, tupleItr) -> {
            List<String> lst = new ArrayList<>();
            while (tupleItr.hasNext())
                lst.add("Partition Num::" + prtnIdx + ", Key::" + tupleItr.next()._1());

            return lst.iterator();
        }, true);

        System.out.println("Range Partitioned Data::" + rangePartnData.collect());


        //Custom Partitioner

        JavaPairRDD<String, String> pairRDD = jsc.parallelizePairs(Arrays.asList(new Tuple2<>("India", "Asia"), new Tuple2<>("Japan", "Asia"),
                new Tuple2<>("Germany", "Europe"), new Tuple2<>("South Africa", "Africa")));

        CustomPartitioner cusp = new CustomPartitioner(3);
        JavaPairRDD<String, String> partedRdd = pairRDD.partitionBy(cusp);

        JavaRDD<String> custPartnData = partedRdd.mapPartitionsWithIndex((prtnIdx, tupleItr) -> {
            List<String> lst = new ArrayList<>();
            while (tupleItr.hasNext())
                lst.add("Partition Num::" + prtnIdx + ", Key::" + tupleItr.next()._1());

            return lst.iterator();
        }, true);

        System.out.println("Custom Partitioned Data::" + custPartnData.collect());

        jsc.close();
    }
}
