package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        List<Integer> inputData = new ArrayList<>();
        inputData.add(35);
        inputData.add(12);
        inputData.add(90);
        inputData.add(20);

        Logger.getLogger("org.apache").setLevel(Level.DEBUG);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> myRdd =  sc.parallelize(inputData);
        Integer result = myRdd.reduce((value1, value2) -> value1 + value2);
        //Spark UI becomes available here
        System.out.println(result);

        JavaRDD<Double> sqrtRdd = myRdd.map( value -> Math.sqrt(value) );
        sqrtRdd.collect().forEach(value -> System.out.println(value));

        //sc.close(); disable it for leaving Spark UI local available.




    }
}