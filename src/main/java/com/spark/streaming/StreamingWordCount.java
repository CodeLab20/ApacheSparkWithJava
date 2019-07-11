package com.spark.streaming;

import com.spark.examples.WordCount;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/*
* https://stackoverflow.com/questions/38252198/spark-streaming-long-queued-active-batches
*
* If you see long queued micro batches in spark web console, master may have loaded heavily. Increase
* number of executors
* */

public class StreamingWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[3]");
        JavaStreamingContext jstrc = new JavaStreamingContext(conf, Durations.seconds(1));

        Logger.getRootLogger().setLevel(Level.WARN);

        JavaReceiverInputDStream<String> txtStream = jstrc.socketTextStream("localhost", 9999, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> wrdsStream = txtStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> wrdCount = wrdsStream.mapToPair(w -> new Tuple2<>(w, 11)).reduceByKey((a, b) -> a + b);
        wrdCount.print();

        jstrc.start();
        jstrc.awaitTermination();

        System.out.println("Done!!!");
    }

}
