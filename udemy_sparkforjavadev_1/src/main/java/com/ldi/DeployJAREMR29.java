package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/** WordCounts example  * **/


public class DeployJAREMR29 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        // remove setMaster("local[*]") for running on a cluster. If you leave setMaster("local[*]") will run only on the driver...not on the cluster
        SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        // Use a file from the S3 or HDFS instead of local text file.
        JavaRDD<String> initialRdd = sc.textFile("s3n://");
        JavaRDD<String> lettersOnly = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]"," ").toLowerCase());
        JavaRDD<String> removeBlankLines = lettersOnly.filter( sentence -> sentence.trim().length()>0);
        JavaRDD<String> words = removeBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> justInterestingWords = words.filter( word -> Util.isNotBoring(word) );
        JavaPairRDD<String, Long> pairsRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L) );
        JavaPairRDD<String, Long> totals = pairsRdd.reduceByKey( (value1, value2) -> value1 + value2  );

        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
        System.out.println(sorted.getNumPartitions());

        List<Tuple2<Long, String>> results= sorted.take(50);
        results.forEach(System.out::println);

        sc.close();

    }
}
