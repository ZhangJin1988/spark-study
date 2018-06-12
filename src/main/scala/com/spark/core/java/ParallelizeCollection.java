package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhangjin
 * @create 2018-06-12 19:49
 */
class ParallelizeCollection {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf()
                .setMaster("locla[1]")
                .setAppName(ParallelizeCollections.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 3, 4, 5, 6, 7);


        JavaRDD<Integer> numberRdd = sc.parallelize(numbers);
        int sum = numberRdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);

        sc.close();
    }
}
