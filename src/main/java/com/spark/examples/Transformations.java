package com.spark.examples;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class Transformations {
    public static void main(String[] args) {
        JavaSparkContext jsc = new JavaSparkContext("local", "Transformations-1");

        List<Integer> intList = Arrays.asList(1,1,2,3,4,5,6,7,8,9,10);

        JavaRDD<Integer> intRdd = jsc.parallelize(intList, 2);

        //Map Transformation
        JavaRDD<Integer> mapRdd = intRdd.map(x -> x + 1);
        System.out.println("Map Transformation:" + mapRdd.collect());

        //Map with Partitions
        JavaRDD<Integer> mapPartitions = intRdd.mapPartitions(itr -> {
            int sum = 0;
            while (itr.hasNext())
                sum += itr.next();
            return Arrays.asList(sum).iterator();
        });
        System.out.print("Map with Partitions:");
        mapPartitions.foreach(x -> System.out.println(x));


        //Map Partitions with Index
        JavaRDD<String> partIdxRdd = intRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer idx, Iterator<Integer> itr) throws Exception {
                int sum = 0;
                StringBuffer sb = new StringBuffer();
                while (itr.hasNext()) {
                    Integer next = itr.next();
                    sb.append(next).append("_");
                    sum += next;
                }
                return Arrays.asList(idx + "::" + sb.toString() + "::" + sum).iterator();
            }
        }, true);

        System.out.println("Map with Partitions Index--");
        partIdxRdd.foreach(x -> System.out.println(x));

        //Filter Operation
        JavaRDD<Integer> filter = intRdd.filter(x -> x % 2 == 1);
        System.out.println("Odd Numbers::" + filter.collect());

        //Flat Map Operation
        JavaRDD<String> strRdd = jsc.parallelize(Arrays.asList("Hello Spark", "Hello Java", "Hello World"));
        JavaRDD<String> sFlatMapRdd = strRdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        System.out.println("Flat Map Data::" + sFlatMapRdd.collect());

        //Map to Pair
        JavaPairRDD<Integer, String > pairRDD = intRdd.mapToPair(x -> x % 2 == 0 ? new Tuple2<>(x, "Even") : new Tuple2<>(x, "Odd"));
        System.out.println("Pair RDD::" + pairRDD.collect());
        System.out.println("Pair Rdd as Map::" + pairRDD.collectAsMap());

        //Flat Map to Pair
        JavaPairRDD<String, Integer> flatMapToPair = strRdd.flatMapToPair(x -> Arrays.asList(
                x.split(" ")).stream()
                .map(t -> new Tuple2<>(t, t.length()))
                .collect(Collectors.toList()).iterator());

        System.out.println("Flat Map to Pair:::" + flatMapToPair.collect());

        //union Operation
        JavaRDD<Integer> intRdd2 = jsc.parallelize(Arrays.asList(11, 12, 3, 14));
        JavaRDD<Integer> union = intRdd.union(intRdd2);
        System.out.println("Union::" + union.collect());

        //Intersection Operation
        JavaRDD<Integer> intRdd3 = jsc.parallelize(Arrays.asList(11, 2, 3, 4));
        JavaRDD<Integer> intersection = intRdd.intersection(intRdd3);
        System.out.println("Intersection::" + intersection.collect());

        jsc.close();
    }
}
