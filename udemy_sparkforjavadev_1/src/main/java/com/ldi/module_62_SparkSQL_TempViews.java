package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

/* udemy module 62 Spark SQL temp view */
public class module_62_SparkSQL_TempViews {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.DEBUG);
        SparkSession spark = SparkSession.builder().appName("module 62").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show(); //read 20 first by default
        System.out.println(dataset.count());

        dataset.createOrReplaceTempView("studentsview");
        Dataset<Row> results = spark.sql("SELECT * FROM studentsview WHERE subject = 'French'");
        results.show();

        //hack for keeping Spark 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
