package com.ldi;

import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class IntegerWithSquareRoot {

    private int originalNumber;
    private double squareRoot;
    public IntegerWithSquareRoot(int originalNumber)  {
        this.originalNumber=originalNumber;
        this.squareRoot = Math.sqrt(originalNumber);
    }
}
