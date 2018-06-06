package com.spark.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhangjin
  * @create 2018-06-05 16:43
  */
object ShowAndEnter {

  def main(args: Array[String]): Unit = {



    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    //
    val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/impclick.txt")



//    val lines: Iterator[String] = Source.fromFile("/Users/zhangjin/myCode/learn/spark-study/impclick.txt").getLines()

    //第一步  拿到list 里面嵌套 tuple

    val datas: RDD[(String, String, String, String)] = lines.flatMap(s => {
      val strs: Array[String] = s.split(",")
      val words: Array[String] = strs(1).replace("|", ",").split(",")
      strs(1).split(",")(1).toDouble
      var lst = scala.collection.mutable.ArrayBuffer[(String, String, String, String)]()
      for (index <- 0 until words.length) {
        val tp: (String, String, String, String) = (strs(0), words(index), strs(2), strs(3))
        lst += tp
      }
      lst
    })

    //第二步 直接分组
    val wordDatas: RDD[((String, String), Iterable[(String, String, String, String)])] = datas.groupBy(tp => (tp._1, tp._2))


    //第三步 累加
    val result: RDD[((String, String), (Int, Int))] = wordDatas.mapValues(value => {
      var sumShow = 0;
      var sumEnter = 0;
      for (v <- value) {
        sumShow = sumShow + v._3.toInt
        sumEnter = sumEnter + v._4.toInt
      }
      (sumShow, sumEnter)
    })
    result.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-study/output7")
//    result.foreach(println)

  }

}
