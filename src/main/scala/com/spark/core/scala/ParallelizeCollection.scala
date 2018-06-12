package com.spark.core.scala

import java.util
import java.util.{Arrays, List}

import com.spark.core.java.ParallelizeCollection
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.Function2

/**
  * @author zhangjin
  * @create 2018-06-12 19:53
  */
object ParallelizeCollection {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("locla[1]").setAppName(ParallelizeCollection.getClass.getSimpleName)

    val sc: SparkContext = new SparkContext(conf)

    val numbers: util.List[Integer] = util.Arrays.asList(1, 3, 4, 5, 6, 7)


    //    val numberRdd: JavaRDD[Integer] = sc.parallelize(numbers)
    //    val sum: Int = numberRdd.reduce(new Function2[Integer, Integer, Integer]() {
    //      @throws[Exception]
    //      override def call(v1: Integer, v2: Integer): Integer = v1 + v2
    //    })
    //
    //    System.out.println(sum)

    sc.stop();
  }

}
