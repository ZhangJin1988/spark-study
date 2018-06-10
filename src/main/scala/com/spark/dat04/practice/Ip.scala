package com.spark.dat04.practice

import com.spark.utils.MySparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-08 21:59
  */
object Ip {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)


    val ipRulesRdd: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/input/ip.txt")


    val longIpRdd: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/input/ipaccess.log")


    longIpRdd



  }


}
