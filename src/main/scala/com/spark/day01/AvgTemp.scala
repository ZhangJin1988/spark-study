package com.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AvgTemp {

  def main(args: Array[String]): Unit = {

    val d1 = Array(("bj", 28.1), ("sh", 28.7), ("gz", 32.0), ("sz", 33.1))
    val d2 = Array(("bj", 27.3), ("sh", 30.1), ("gz", 33.3))
    val d3 = Array(("bj", 28.2), ("sh", 29.1), ("gz", 32.0), ("sz", 30.5))

//    val sparkconf = new sparkConf
    val conf : SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    //
    val sc:SparkContext = new SparkContext(conf)

    val tuples: Array[(String, Double)] = d1++d2++d3
    val tuplesRdd: RDD[(String, Double)] = sc.makeRDD(tuples)
    val valuesRdd : RDD[(String, Iterable[(String, Double)])] = tuplesRdd.groupBy(_._1)

    val result: RDD[(String, Double)] = valuesRdd.map(kv => {
      val sum: Double = kv._2.toList.map(_._2).sum
      val length: Int = kv._2.toList.length
      (kv._1, sum / length)
    }).sortBy(_._2)
//    result.foreach(println)
    result.saveAsTextFile("/Users/zhangjin/myCode/learn/sparkstudy/output4")
//    sc.parallelize()
    sc.stop()
//    val by: Map[String, Array[(String, Double)]] = tuples.groupBy(_._1)
//    val values: Map[String, Double] = by.mapValues(kv => {
//      val sum: Double = kv.map(_._2).sum
//      val length: Int = kv.length
//      sum / length
//
//    })
//    values.toList.sortBy(-_._2).foreach(println)
  }

}
