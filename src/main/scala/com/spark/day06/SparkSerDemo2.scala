package cn.edu360.spark32.day06

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Huge
  * DATE: 2018/6/10
  * Desc: 
  */

object SparkSerDemo2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    // 是否会报错？？  会报错  MyTask的类 必须要实现序列化
    // MyTask 是在哪里创建的？  在driver端创建了一个实例
    val mt = new MyTask()

    println(s"-----------------@@@@@@${mt.toString}@@@@@-----------------------")

    val file = sc.textFile(args(0))
    file.map(str => {
      val hstName = InetAddress.getLocalHost.getHostName

      val tName: String = Thread.currentThread().getName

      (hstName, tName, mt.mp.getOrElse(str, -1), mt.toString)
    }).saveAsTextFile(args(1))

    sc.stop()
  }
}
