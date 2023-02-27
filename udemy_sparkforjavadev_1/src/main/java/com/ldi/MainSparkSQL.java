package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

/* udemy module 57 Spark SQL */
public class MainSparkSQL {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        //JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().appName("testing Spark SQL").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        spark.read().csv("src/main/resources/exams/students.csv");
        //hack for keeping Spark 4040 running
        //Scanner scanner = new Scanner(System.in);
        //scanner.nextLine();

        spark.close();

    }
}
