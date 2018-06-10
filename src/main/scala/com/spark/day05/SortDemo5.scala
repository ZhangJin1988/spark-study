package com.spark.day05

import com.spark.utils.MySparkUtils
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-09 10:21
  */
object SortDemo5 {

  def main(args: Array[String]): Unit = {


    val sc = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    //商品数据为示例子
    val product: RDD[String] = sc.makeRDD(List("shouji 2999.9 10", "shouzhi 3.5 10000", "shoubiao 999.99 1000", "lazhu 9.9 10000","pibian 999.99 1300"))


    //按照一个条件 商品的价格降序

    val tpRdd: RDD[(String, Double, Int)] = product.map(str => {
      val split: Array[String] = str.split(" ")
      val name = split(0)
      val price = split(1).toDouble
      val amout = split(2).toInt
      (name, price, amout)
    })
    //排序 按照 价格降序 仅仅是在排序的时候 利用类的规则

    //当类没有实现比较器的时候 可以用隐式转换 来实现

    //排序 按照 价格降序 仅仅是在排序的时候 利用类的规则
    //隐士转换 把mp3 转换成Ordered
    implicit def product32OrderedProduct(mp3: MyProduct3): Ordered[MyProduct3] = {
      new Ordered[MyProduct3] {
        override def compare(that: MyProduct3): Int = {

          if (mp3.price == that.price) {
            mp3.amount - that.amount
          } else {
            if (that.price - mp3.price >= 0) 1 else -1
          }
        }
      }
    }

    val result: RDD[(String, Double, Int)] = tpRdd.sortBy(t=>MyProduct3(t._1,t._2,t._3))

    result.coalesce(1).foreach(println)


    sc.stop()

  }
}


case class MyProduct3(val pName: String,  val price: Double,  val amount: Int) {
//  override def compare(that: MyProducts): Int = {
//
//    if (this.price == that.price) {
//      this.amount - that.amount
//    } else {
//      if (that.price - this.price >= 0) 1 else -1
//    }
//  }


  override def toString = s"MyProduct($pName, $price, $amount)"
}

