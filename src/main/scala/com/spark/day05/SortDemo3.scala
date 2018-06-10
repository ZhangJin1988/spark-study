package com.spark.day05

import com.spark.utils.MySparkUtils
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-09 09:45
  *
  *         利用类来封装数据
  *
  *
  */
object SortDemo3 {


  def main(args: Array[String]): Unit = {


    val sc = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    //商品数据为示例子
    val product: RDD[String] = sc.makeRDD(List("shouji 2999.9 10", "shouzhi 3.5 10000", "shoubiao 999.99 1000", "lazhu 9.9 10000", "pibian 999.99 1300"))


    //按照一个条件 商品的价格降序

    val tpRdd: RDD[MyProduct1] = product.map(str => {
      val split: Array[String] = str.split(" ")
      val name = split(0)
      val price = split(1).toDouble
      val amout = split(2).toInt
      new MyProduct1(name, price, amout)
    })
    //排序 按照 价格降序
    tpRdd.sortBy(t => t).coalesce(1).foreach(println)


    sc.stop()
  }

}


/**
  * 自定义排序的类 要跨节点传输 要实现序列话的通知
  *
  * 可以用case class  默认就实现了 序列化的特质 就不需要实现 serializable
  * @param pName
  * @param price
  * @param amount
  */

class MyProduct1(val pName: String, val price: Double, val amount: Int) extends Ordered[MyProduct1] with Serializable {
  override def compare(that: MyProduct1): Int = {

    if (this.price == that.price) {
      this.amount - that.amount
    } else {
      if (that.price - this.price >= 0) 1 else -1
    }
  }


  override def toString = s"MyProduct($pName, $price, $amount)"
}
