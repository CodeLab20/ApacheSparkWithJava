package com.spark.examples;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Transformations3 {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "Transformations-2");

        List<Integer> intList = Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15);

        JavaRDD<Integer> intRdd = jsc.parallelize(intList, 3);

        //mapPartitions
        JavaRDD<Integer> plus5Rdd = intRdd.mapPartitions(itr -> {
            List<Integer> res = new ArrayList<>();
            while (itr.hasNext())
                res.add(itr.next() + 5);

            return res.iterator();
        });

        System.out.println("Map Partitions Data ::" + plus5Rdd.collect());

        //MapPartitionsWithIndex
        JavaRDD<String> partnIdxRdd = intRdd.mapPartitionsWithIndex((pIndex, rddItr) -> {
            List<String> res = new ArrayList<>();
            while (rddItr.hasNext())
                res.add("Element " + rddItr.next() + ", Partition Num:" + pIndex);
            return res.iterator();
        }, false);

        System.out.println("Map Partitions with Index Data ::" + partnIdxRdd.collect());


        //Map Partitions to Pair
        JavaPairRDD<String, Integer> oddEvnRdd = intRdd.mapPartitionsToPair(rddItr -> {
            List<Tuple2<String, Integer>> res = new ArrayList<>();
            while (rddItr.hasNext()) {
                int num = rddItr.next();
                String key = num % 2 == 0 ? "Even" : "Odd";
                res.add(new Tuple2<>(key, num));
            }
            return res.iterator();
        });

        System.out.println("Map Partitions to Pair Data::" + oddEvnRdd.collect());


        //Map Values
        JavaPairRDD<String, Integer> mult3Rdd = oddEvnRdd.mapValues(i -> i * 3);
        System.out.println("Map Values Data::" + mult3Rdd.collect());

        //Flat Map Values
        JavaPairRDD<String, String> monthExpenses = jsc.parallelizePairs(Arrays.asList(
                            new Tuple2<>("Jan", "25,5,75,10"), new Tuple2<>("Feb", "30,6,90,12")));

        JavaPairRDD<String, Integer> month1Val = monthExpenses.flatMapValues(s -> Arrays.asList(s.split(","))
                .stream().map(t -> Integer.valueOf(t))
                .collect(Collectors.toList()));

        System.out.println("Flat Map Values Data::" + month1Val.collect());


        //Repartition and sort within partitions
        JavaPairRDD<String, Integer> sortWithinPartitions = month1Val.repartitionAndSortWithinPartitions(new HashPartitioner(2));
        System.out.println("Repartition and sort Within Partitions Data::" + sortWithinPartitions.collect());

        //coalesce
        JavaRDD<Integer> coalesceRdd = intRdd.coalesce(2);
        System.out.println("Coalesce Data::" + coalesceRdd.collect());

        //Fold By Key
        JavaRDD<String> strRdd = jsc.parallelize(Arrays.asList("Hello Spark", "Hello Java", "Hello World"));
        JavaPairRDD<String, Integer> strPairRDD = strRdd.flatMapToPair(s -> Arrays.asList(s.split(" "))
                .stream().map(t -> new Tuple2<>(t, 1))
                .collect(Collectors.toList()).iterator());

        JavaPairRDD<String, Integer> sumStrPairRdd = strPairRDD.foldByKey(0, (i1, i2) -> i1 + i2);

        System.out.println("Fold By Key Data::" + sumStrPairRdd.collect());


        //Aggregate by key
        JavaPairRDD<String, String> areas = jsc.parallelizePairs(Arrays.asList(
                new Tuple2<>("K1", "Austria"), new Tuple2<>("K2", "Australia"),
                new Tuple2<>("K3", "Antartica"), new Tuple2<>("K1", "Asia"),
                new Tuple2<>("K2", "France"), new Tuple2<>("K3", "Canada"),
                new Tuple2<>("K1", "Argentina"), new Tuple2<>("K2", "America"),
                new Tuple2<>("K3", "Germany"), new Tuple2<>("K2", "Africa") ));

        /*JavaPairRDD<String, Integer> aggrKeyAreas = areas.aggregateByKey(0, (v1, v2) -> {
            System.out.println(v1 + "~~~~" + v2);
            if (v2.startsWith("A"))
                v1 += 1;
            return v1;
        }, (v1, v2) -> v1 + v2);

        System.out.println("Aggregate By Key Data:::" + aggrKeyAreas.collect());*/

        JavaPairRDD<String, ArrayList<Object>> aggrKeyAreas2 = areas.aggregateByKey(new ArrayList<>(), (v1, v2) -> {
            System.out.println(v1 + "~~~~" + v2);   //this function is for single partition aggregate
            v1.add(v2);
            return v1;
        }, (v1, v2) -> {    //this function is for inter partition aggregate
            v1.addAll(v2);
            return v1;
        });

        System.out.println("Aggregate By Key Data:::" + aggrKeyAreas2.collect());


        //Combine by Key
        JavaPairRDD<String, String> areasCombined = areas.combineByKey(c -> String.valueOf(c), (c, s) -> c + "|" + s, (c, c2) -> c + "|" + c2);
        System.out.println("Combine By Key Data::" + areasCombined.collect());

        jsc.close();
        System.out.println("Done!!!");
    }
}
