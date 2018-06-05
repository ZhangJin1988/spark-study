package com.java.day02;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaWordCount {

    public static void main(String[] args) {


        if(args.length !=2){
            System.out.println("Usage ! com.java.day02.JavaWordCount <input> <output> ");
        }

        SparkConf conf = new SparkConf();

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> stringJavaRDD = sparkContext.textFile(args[0]);




    }
}
