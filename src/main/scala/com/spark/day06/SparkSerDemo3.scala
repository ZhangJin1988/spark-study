package cn.edu360.spark32.day06

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Huge
  * DATE: 2018/6/10
  * Desc: 
  */

object SparkSerDemo3 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    val mt = new MyTask()
    // 类还是需要进行序列化
    val mymt = sc.broadcast(mt)

    println(s"-----------------@@@@@@${mt.toString}@@@@@-----------------------")

    val file = sc.textFile(args(0))
    file.map(str => {
      val hstName = InetAddress.getLocalHost.getHostName

      // 获取广播变量的值
      val newMt: MyTask = mymt.value
      val tName: String = Thread.currentThread().getName

      (hstName, tName, newMt.mp.getOrElse(str, -1), newMt.toString)
    }).saveAsTextFile(args(1))

    sc.stop()
  }
}
