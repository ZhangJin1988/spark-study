package com.spark.day03

import com.spark.utils.MySparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-06 09:21
  */
object SortDemo {
  def main(args: Array[String]): Unit = {


    val d1 = Array(("bj",28.1),("sh",28.7),("gz",32.0),("sz",33.1))


    val tmp:Array[Double] = d1.map(_._2)
    val sc: SparkContext = MySparkUtils.getSparkContext(this.getClass.getSimpleName)


    val rdd1: RDD[(String, Double)] = sc.makeRDD(d1)
    val res1: RDD[(String, Double)] = rdd1.sortBy(_._1)
    val res2: RDD[(String, Double)] = rdd1.sortByKey(false)


    res1.foreach(println)

    println("---------")

    res2.foreach(println)


    sc.stop()




  }



}
