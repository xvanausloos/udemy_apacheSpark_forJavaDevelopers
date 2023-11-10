package com.ldi;

import io.netty.handler.codec.serialization.ObjectEncoder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

public class module_69_pivotTables {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("module 69").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        Dataset<Row> dataset = spark.read().option("header","true").csv("src/main/resources/biglog.txt");

        //dataset = dataset.selectExpr("level", "date_format(datetime,'MMMM') as month");
        dataset = dataset.select(col("level"),
                                    date_format(col("datetime"), "MMMM").alias("month"),
                                    date_format(col("datetime"),"M").alias("monthnum").cast(DataTypes.IntegerType));

        Object[] months =  new Object[] {"January", "February", "March", "April", "May", "June", "July",
                "Augusceemt", "September", "October", "November", "December"};

        List<Object> columns = Arrays.asList(months);
        dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);

        dataset.show(100);

        //hack for keeping Spark UI 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
