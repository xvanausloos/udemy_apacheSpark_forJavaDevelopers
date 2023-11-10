package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Scanner;

/* udemy module 67  */
public class module_67_SparkSQL_ordering {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("module 67 ordering").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        Dataset<Row> dataset = spark.read().option("header","true").csv("src/main/resources/biglog.txt");
        //dataset.show(200);

        dataset.createOrReplaceTempView("logging_table");

        //Dataset<Row> results = spark.sql("SELECT level, date_format(datetime,'MMMM') as month, cast(first(date_format(datetime, 'M')) as integer) as nummonth, count(1) AS total" +
                //" FROM logging_table group by level, month order by nummonth");

        Dataset<Row> results = spark.sql("SELECT level, date_format(datetime,'MMMM') as month, count(1) AS total" +
                " FROM logging_table group by level, month order by cast(first(date_format(datetime, 'M')) as integer), level");


        results = results.drop("nummonth");
        results.show(100);

        //hack for keeping Spark UI 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
