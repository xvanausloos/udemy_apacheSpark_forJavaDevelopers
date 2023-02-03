package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/* Udemy Spark for Java Dev - Module 8 */
public class Tupples13 {
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

        //JavaRDD<Double> sqrtRdd = originalIntegers.map(value -> Math.sqrt(value) );
        //sqrtRdd.collect().forEach(System.out::println);
        //use this fix for issue when get not serializable exception multiple CPUS etc.
        //do not use sqrtRdd.foreach(System.out::println);

        JavaRDD<IntegerWithSquareRoot> sqrtRdd = originalIntegers.map(value -> new IntegerWithSquareRoot(value));

        JavaRDD<Tuple2> sqrtRdd2 = originalIntegers.map(value -> new Tuple2(value, Math.sqrt(value)));
        sqrtRdd2.collect().forEach(System.out::println);
        System.out.println("End");
        sc.close(); // disable it for leaving Spark UI local available.




    }
}