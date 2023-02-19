package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Use of file as input from Disk
 * **/


public class ReadingFromDisk21 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        JavaRDD<String> sentences = initialRdd;
        JavaRDD<String> filteredWords = sentences.flatMap( value -> Arrays.asList(value.split(" ")).iterator()).filter( word -> word.length() > 1);
        filteredWords.collect().forEach(System.out::println);


        sc.close();

    }
}
