package com.spark.day05

import com.spark.utils.MySparkUtils
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-09 09:34
  *        按照单条件的排序 如果要多条件
  */
object SortDemo1 {

  def main(args: Array[String]): Unit = {


    val sc = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    //商品数据为示例子
    val product: RDD[String] = sc.makeRDD(List("shouji 2999.9 10","shouzhi 3.5 10000","shoubiao 999.99 1000","lazhu 9.9 10000"))


    //按照一个条件 商品的价格降序

    val tpRdd: RDD[(String, Double, Int)] = product.map(str => {
      val split: Array[String] = str.split(" ")
      val name = split(0)
      val price = split(1).toDouble
      val amout = split(2).toInt
      (name, price, amout)
    })
    //排序 按照 价格降序
    tpRdd.sortBy(-_._2).coalesce(1).foreach(println)








    sc.stop()
  }

}
