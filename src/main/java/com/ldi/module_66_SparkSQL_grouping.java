package com.ldi;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;


/* udemy module 65 Spark SQL date formatting  */
public class module_66_SparkSQL_grouping {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                .appName("module 66 multiple groupings")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","/tmpspark")
                .getOrCreate(); //initiate Spark with Spark SQL different than SparkConf



        //
        //STAGE 1 inMemory
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

        /*Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);

        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'MMMM') AS month, count(1) AS total FROM logging_table GROUp BY level, month");
        results.show();
        */

        // STAGE 2 large log file
        Dataset<Row> dataset = spark.read().option("header","true").csv("src/main/resources/biglog.txt");
        //dataset.show(10);

        dataset.createOrReplaceTempView("logging_table");

        // ERROR HERE
        //Dataset<Row> results = spark.sql("SELECT level, date_format(datetime,'MMMM') as month, count(1) FROM logging_table");
        Dataset<Row> results = spark.sql("SELECT level, date_format(datetime, 'MMMM') as month FROM logging_table ");
        //System.out.println(results.count());

        results.createOrReplaceTempView("logging_table");

        results = spark.sql("SELECT level, month, count(1) as total FROM logging_table GROUP BY level, month");
        results.show();

        //Dataset<Row> totals = spark.sql("SELECT sum(total) FROM results_table");
        //totals.show();

        //hack for keeping Spark UI 4040 running
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        spark.close();

    }
}
