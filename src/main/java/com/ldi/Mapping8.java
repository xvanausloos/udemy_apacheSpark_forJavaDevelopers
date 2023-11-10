package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.List;

/* Udemy Spark for Java Dev - Module 8 */
public class Mapping8 {
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
        System.out.println(result);

        JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value) );
        //sqrtRdd.foreach(value -> System.out.println(value));
        sqrtRdd.collect().forEach(System.out::println);
        //use this fix for issue when get not serializable exception multiple CPUS etc.
        //do not use sqrtRdd.foreach(System.out::println);

        //how many elements
        System.out.println(sqrtRdd.count());

        // Count using Map and Reduce
        JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        Long count = singleIntegerRdd.reduce( (value1, value2) -> value1 + value2);
        System.out.println(count);
        System.out.println("End");
        //sc.close(); disable it for leaving Spark UI local available.




    }
}