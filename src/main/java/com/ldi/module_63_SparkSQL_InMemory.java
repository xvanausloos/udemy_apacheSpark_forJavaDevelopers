package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/* udemy module 63 Spark SQL in memory RowFactory class is doing this job. Can be useful for unit test  */
public class module_63_SparkSQL_InMemory {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("module 63").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf

        List<Row> inMemory = new ArrayList<Row>();

        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));


        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };
        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.show();



        //group by level
        dataset.createOrReplaceTempView("logging_table");
        //Dataset<Row> results = spark.sql("SELECT level, COUNT(datetime) FROM logging_table GROUP BY level ORDER BY level");
        Dataset<Row> results = spark.sql("SELECT level, collect_list(datetime) FROM logging_table GROUP BY level ORDER BY level");
        results.show(true);
        //hack for keeping Spark UI 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
