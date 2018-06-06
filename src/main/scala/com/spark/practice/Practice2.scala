package com.spark.practice

import com.spark.utils.MySparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * @author zhangjin
  * @create 2018-06-06 11:39
  */
object Practice2 {

  /**
    *
    * 两个数据集：
    * * 数据集A		 id age name
    * * 数据集B    id year month movie
    * 数据集都是使用空格来分割的
    *
    * "".split(" ",2)  选择好API    一定要提交到集群中运行一下，有没有问题
    * * 要求，输出：id age name,year month movie(同一个用户，按year升序，没有数据B的id，用null补
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {


    val sc: SparkContext = MySparkUtils.getSparkContext(this.getClass.getSimpleName)

    val aRdd: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/a.txt")
    val bRdd: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/b.txt")


    val aDatas: RDD[(String, (String, String))] = aRdd.map(k => {
      val strs: Array[String] = k.split(" ")
      val id = strs(0)
      val age = strs(1)
      val name = strs(2)
      (id, (age, name))
    })

    val bDatas: RDD[(String, Iterable[(String, String, String, String)])] = bRdd.map(k => {
      val strs: Array[String] = k.split(" ")
      var id = strs(0)
      var year = strs(1)
      var month = strs(2)
      var movie = strs(3)
      (id, year, month, movie)
    }).groupBy(_._1)
    val datas: RDD[(String, ((String, String), Option[Iterable[(String, String, String, String)]]))] = aDatas.leftOuterJoin(bDatas)

    val result: RDD[(String, String, String, String, String, String)] = datas.flatMap(kv => {
      val id = kv._1
      val age = kv._2._1._1
      val name = kv._2._1._2
      import scala.collection.mutable.ArrayBuffer
      //      var lst: Array[(String,String,String,String,String,String)] = Array()
      var lst = new ArrayBuffer[(String, String, String, String, String, String)]();
      if (kv._2._2.isEmpty) {
        val tuple: (String, String, String, String, String, String) = (id.toString, age.toString, name.toString, "null".toString, "null".toString, "null".toString)
        lst += tuple
        //        lst.++(tuple)
      } else {
        for (i <-kv._2._2.get.toList) {
          val tuple: (String, String, String, String, String, String) = (id, age, name,i._1, i._2, i._3)
          lst += tuple
        }
      }

      lst


    })

    result.foreach(println)
  }


}
