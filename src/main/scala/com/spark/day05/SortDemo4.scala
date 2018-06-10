package com.spark.day05

import com.spark.utils.MySparkUtils
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-09 10:15
  */
object SortDemo4 {

  def main(args: Array[String]): Unit = {


    val sc = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    //商品数据为示例子
    val product: RDD[String] = sc.makeRDD(List("shouji 2999.9 10", "shouzhi 3.5 10000", "shoubiao 999.99 1000", "lazhu 9.9 10000", "pibian 999.99 1300"))


    //按照一个条件 商品的价格降序

    val tpRdd: RDD[(String, Double, Int)] = product.map(str => {
      val split: Array[String] = str.split(" ")
      val name = split(0)
      val price = split(1).toDouble
      val amout = split(2).toInt
      (name, price, amout)
    })


    val result: RDD[(String, Double, Int)] = tpRdd.sortBy(t => MyProduct(t._1, t._2, t._3))

    result.coalesce(1).foreach(println)


    sc.stop()

  }

}

case class MyProduct(val pName: String, val price: Double, val amount: Int) extends Ordered[MyProduct] with Serializable {
  override def compare(that: MyProduct): Int = {

    if (this.price == that.price) {
      this.amount - that.amount
    } else {
      if (that.price - this.price >= 0) 1 else -1
    }
  }


  override def toString = s"MyProduct($pName, $price, $amount)"
}
