package com.spark.files.txt;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class TextFileOperations {

    public static void main(String[] args) {
        SparkConf sc = new SparkConf()
                            .setMaster("local")
                            .setAppName("TextFileOperations")
                            .set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext jsc = new JavaSparkContext(sc);

        JavaRDD<String> file = jsc.textFile("InputFiles/persons.txt");
        JavaRDD<Person> personRDD = file.mapPartitions(lines ->
                                        {
                                            List<Person> persons = new ArrayList<>();
                                            while (lines.hasNext()) {
                                                String[] pInfo = lines.next().split("~");
                                                Person p = new Person();
                                                p.setName(pInfo[0]);
                                                p.setAge(Integer.parseInt(pInfo[1]));
                                                p.setJob(pInfo[2]);
                                                persons.add(p);
                                            }
                                            return persons.iterator();
                                        });

        List<Person> persons = personRDD.collect();

        personRDD.saveAsTextFile("Persons/pInfo");

        List<String> personData = new ArrayList<>();
        persons.stream().forEach(p -> {
            personData.add(p.getName());
            personData.add(String.valueOf(p.getAge()));
            personData.add(p.getJob());
        });

        JavaRDD<String> personStrs = jsc.parallelize(personData);

        personStrs.saveAsTextFile("Persons/personInfo");
        personStrs.repartition(2).saveAsTextFile("Persons/repartioned");
        personStrs.coalesce(1).saveAsTextFile("Persons/coalesced");

        System.out.println("Done!!!");

    }
}
