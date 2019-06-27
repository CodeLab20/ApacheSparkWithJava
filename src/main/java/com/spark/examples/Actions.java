package com.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class Actions {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "Actions-2");

        List<Integer> intList = Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> intRdd = jsc.parallelize(intList);

        //isEmpty
        boolean empty = intRdd.filter(a -> a == 6).isEmpty();
        System.out.println("Is Empty::" + empty);

        //Collect as map
        JavaPairRDD<String, Integer> pairRdd1 = jsc.parallelizePairs(Arrays.asList(
                                                    new Tuple2("A", 1), new Tuple2("C", 3),
                                                    new Tuple2("D", 4), new Tuple2("B", 2),
                                                    new Tuple2("A", 5), new Tuple2("E", 3), new Tuple2("B", 2)));

        Map<String, Integer> map = pairRdd1.collectAsMap();
        System.out.println("Collected Map is::" + map);

        //count
        System.out.println("Count is::" + intRdd.count());

        //count by key
        Map<String, Long> countByKey = pairRdd1.countByKey();
        System.out.println("Count By Key::" + countByKey);

        //count by value
        Map<Tuple2<String, Integer>, Long> countByValue = pairRdd1.countByValue();
        System.out.println("Count By Value::" + countByValue);

        //max
        Integer max = intRdd.max(Comparator.naturalOrder());
        System.out.println("Max Num::" + max);

        //min
        Integer min = intRdd.min(Comparator.naturalOrder());
        System.out.println("Min Num::" + min);

        //first
        System.out.println("First::" + intRdd.first());

        //take
        System.out.println("2 Taken as::" + intRdd.take(2));

        // Take ordered
        System.out.println("3 taken ordered as::" + intRdd.takeOrdered(3));

        // Take ordered reverse
        System.out.println("3 Taken as reverse ordered as::" + intRdd.takeOrdered(3, Comparator.reverseOrder()));

        jsc.close();
    }
}
