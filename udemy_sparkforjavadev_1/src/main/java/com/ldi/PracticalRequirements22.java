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

import static com.ldi.Util.isNotBoring;

/** WordCounts example  * **/


public class PracticalRequirements22 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
        JavaRDD<String> lettersOnly = initialRdd.map( sentence -> sentence.replaceAll("[^a-zA-Z\\s]"," ").toLowerCase());
        JavaRDD<String> removeBlankLines = lettersOnly.filter( sentence -> sentence.trim().length()>0);
        JavaRDD<String> words = removeBlankLines.flatMap( sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> justInterestingWords = words.filter( word -> Util.isNotBoring(word) );
        JavaPairRDD<String, Long> pairsRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L) );
        JavaPairRDD totals = pairsRdd.reduceByKey( (value1, value2) -> value1 + value2  );

        List<Tuple2<String, Long>> results= totals.take(50);
        results.forEach(System.out::println);

        sc.close();

    }
}
