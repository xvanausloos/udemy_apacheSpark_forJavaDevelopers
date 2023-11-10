package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/* udemy module 57 Spark SQL */
public class MainSparkSQL_57 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //SparkConf conf = new SparkConf().setAppName("reduceRDD example").setMaster("local[*]");
        //JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().appName("testing Spark SQL").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
        dataset.show(); //read 20 first by default

        long numberOfRows = dataset.count();
        System.out.println("Number of Rows : " + numberOfRows);

        Row firstRow = dataset.first();
        String subject = firstRow.get(2).toString();
        System.out.println(subject);

        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(" The year was " + year);


        //hack for keeping Spark 4040 running
        //Scanner scanner = new Scanner(System.in);
        //scanner.nextLine();

        spark.close();

    }
}
