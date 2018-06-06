package com.spark.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-06 09:24
  */
object MySparkUtils {


  def getSparkContext(appName: String): SparkContext = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(appName)
    //
    val sc: SparkContext = new SparkContext(conf)

    sc
  }
}
