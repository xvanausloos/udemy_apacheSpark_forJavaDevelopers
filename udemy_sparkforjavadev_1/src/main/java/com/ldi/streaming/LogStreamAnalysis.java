package com.ldi.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import javax.naming.event.ObjectChangeListener;

public class LogStreamAnalysis {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("logStream").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(30));
        JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost",8989);
        JavaDStream<Object> results = inputData.map(item -> item);
        results.print();

        sc.start();
        sc.awaitTermination();
    }
}
