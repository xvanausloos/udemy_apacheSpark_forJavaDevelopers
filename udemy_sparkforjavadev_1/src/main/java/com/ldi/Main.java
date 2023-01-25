package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

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


        JavaRDD<Integer> originalIntegers =  sc.parallelize(inputData);
        Integer result = originalIntegers.reduce((value1, value2) -> value1 + value2);
        //Spark UI becomes available here
        System.out.println(result);

        //JavaRDD<Double> sqrtRdd = originalIntegers.map( value -> Math.sqrt(value) );
        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value)));
        sqrtRdd.foreach(System.out::println); //Java 8 sugar syntax
        //sqrtRdd.forEach(value -> System.out.println(value));
        System.out.println(sqrtRdd.count());

        Tuple2<Integer, Double> myValue = new Tuple2<Integer, Double>(9, 3.0);


        //sc.close(); disable it for leaving Spark UI local available.




    }
}