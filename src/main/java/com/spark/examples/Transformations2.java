package com.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Transformations2 {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "Transformations-2");

        List<Integer> intList = Arrays.asList(1,1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> intRdd = jsc.parallelize(intList, 2);
        /*Repartition Operation*/JavaPairRDD<String, Integer> pairRDD = intRdd.mapToPair(i -> new Tuple2<>(i % 2 == 0 ? "Even" : "Odd", i));
        JavaPairRDD<String, Integer> partitionedRDD = pairRDD.repartition(2);
        System.out.println("Repartitoned RDD::" + partitionedRDD.collect());

        //distinct Operation
        JavaRDD<Integer> distinct = intRdd.distinct();
        System.out.println(distinct.collect());

        /*Cartesian Operation*/
        JavaRDD<String> strRDD = jsc.parallelize(Arrays.asList("A", "B", "C", "D"));
        JavaRDD<Integer> iRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4));
        JavaPairRDD<String, Integer> cartesian = strRDD.cartesian(iRdd);
        System.out.println("Cartesian Product::" + cartesian.collect());

        //GroupByKey
        JavaPairRDD<String, Iterable<Integer>> groupByKey = pairRDD.groupByKey();
        System.out.println("Group By Key::" + groupByKey.collect());

        //reduce by key
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey((i1, i2) -> i1 + i2);
        System.out.println("reduce by key::" + reduceByKey.collect());

        //sort by key
        JavaPairRDD<String, Integer> sortByKey = pairRDD.sortByKey();
        System.out.println("Sort By Key:::" + sortByKey.collect());

         //Join Operations
        JavaPairRDD<String, String> pairRdd1 = jsc.parallelizePairs(Arrays.asList(
                                                new Tuple2("A", "B"), new Tuple2("C", "D"),
                                                new Tuple2("D", "E"), new Tuple2("B", "A")));

        JavaPairRDD<String, Integer> pairRDD2 = jsc.parallelizePairs(Arrays.asList(
                new Tuple2("B", 2), new Tuple2("C", 5),
                new Tuple2("D", 7), new Tuple2("A", 9)));

            //inner join
            JavaPairRDD<String, Tuple2<String, Integer>> join = pairRdd1.join(pairRDD2);
            System.out.println("Join::" + join.collect());

            //Left Outer Join
            JavaPairRDD<String, Tuple2<String, Optional<Integer>>> loJoin = pairRdd1.leftOuterJoin(pairRDD2);
            System.out.println("Left Outer Join::" + loJoin.collect());

            //Right Outer Join
            JavaPairRDD<String, Tuple2<Optional<String>, Integer>> roJoin = pairRdd1.rightOuterJoin(pairRDD2);
            System.out.println("Right Outer Join::"+ roJoin.collect());

            //Full Outer Join
            JavaPairRDD<String, Tuple2<Optional<String>, Optional<Integer>>> foJoin = pairRdd1.fullOuterJoin(pairRDD2);
            System.out.println("Full Outer Join::" + foJoin.collect());

        //Co Group Operations
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = pairRdd1.cogroup(pairRDD2);
        System.out.println("Co-Group::" + cogroup.collect());

        //Group With Operations
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<Integer>>> groupWith = pairRdd1.groupWith(pairRDD2);
        System.out.println("Group With::" + groupWith.collect());


        jsc.close();
    }
}
