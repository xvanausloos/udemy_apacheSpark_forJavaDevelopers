package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class module_74_lambda_udf {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("module 72").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        spark.udf().register("hasPassed", (String grade) -> grade.equals("A+"), DataTypes.BooleanType);
        Dataset<Row> dataset = spark.read().option("header","true").csv("src/main/resources/exams/students.csv");

        dataset = dataset.withColumn("pass", functions.callUDF("hasPassed", col("grade")));
        dataset.show();



        //hack for keeping Spark UI 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
