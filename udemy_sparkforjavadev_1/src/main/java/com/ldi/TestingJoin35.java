package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/** Testing right outer join example  * **/


public class TestingJoin35 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(4,18));
        visitsRaw.add(new Tuple2<>(6,4));
        visitsRaw.add(new Tuple2<>(10,9));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD users = sc.parallelizePairs(usersRaw);
        JavaPairRDD visits = sc.parallelizePairs(visitsRaw);

        JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> joinRdd = visits.rightOuterJoin(users);
        joinRdd.collect().forEach( it -> System.out.println("user "+ it._2._2 + " had " + it._2._1.orElse(0)));
        sc.close();

    }
}