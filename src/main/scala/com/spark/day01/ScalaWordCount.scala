package com.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {

  def main(args: Array[String]): Unit = {


    if(args.length != 2){
      println("Usage: com.spark.day01.ScalaWordCount <input> <output> ")
      sys.exit(1)
    }

    val Array(input,output) = args

    val conf : SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
//
    val sc:SparkContext = new SparkContext(conf)

    val file:RDD[String] = sc.textFile(input)

    val splitedrdd:RDD[String] = file.flatMap(_.split(" "))

    val wordWithOne:RDD[(String,Int)] = splitedrdd.map((_,1))

    val result : RDD[(String,Int)] = wordWithOne.reduceByKey(_ + _)

    val finalRes : RDD[(String,Int)] = result.sortBy(-_._2)

    finalRes.saveAsTextFile(output)

    //关闭资源
    sc.stop()





  }

}
