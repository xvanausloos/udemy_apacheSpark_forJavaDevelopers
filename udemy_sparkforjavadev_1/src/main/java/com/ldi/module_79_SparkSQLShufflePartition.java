package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class module_79_SparkSQLShufflePartition {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("module 68").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different from SparkConf

        //adjust settings for avoiding empty partitions. Avoid idle tasks for empty partitions
        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header","true").csv("src/main/resources/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");

        //this version use SHORTAGGREGATE algorithm
        Dataset<Row> results = spark.sql
                ("SELECT level, date_format(datetime,'MMMM') AS month, count(1) AS total, first(date_format(datetime,'M')) AS monthnum" +
                " FROM logging_table GROUP BY level, month ORDER BY cast(monthnum AS int), level");
        // results.drop("monthnum");

        //dataset = dataset.selectExpr("level", "date_format(datetime,'MMMM') as month");
/*
        dataset = dataset.select(col("level"),
                                    date_format(col("datetime"), "MMMM").alias("month"),
                                    date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));
        dataset = dataset.groupBy(col("level"),col("month"),col("monthnum")).count();
        dataset = dataset.orderBy("monthnum", "level");
        dataset = dataset.drop("monthnum");
        dataset.show(100);
*/
        results.show(100);
        results.explain();

        //hack for keeping Spark UI 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
