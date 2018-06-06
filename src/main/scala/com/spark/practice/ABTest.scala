package com.spark.practice

import com.spark.utils.MySparkUtils
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-06 14:49
  */
object ABTest {

  def main(args: Array[String]): Unit = {

    val sc = MySparkUtils.getSparkContext(this.getClass.getSimpleName)

    val aData: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/a.txt")
    val bData: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/b.txt")


    val aDataSplit: RDD[(String, String)] = aData.map(t => {
      val split: Array[String] = t.split(" ", 2)
      (split(0), split(1))
    })

    val bDataSplit: RDD[(String, String)] = bData.map(t => {
      val split: Array[String] = t.split(" ", 2)
      (split(0), split(1))
    })
    val cogroup: RDD[(String, (Iterable[String], Iterable[String]))] = aDataSplit.cogroup(bDataSplit)

    val result: RDD[(String, String)] = cogroup.mapValues(tp => {
      //从第一个元素中 获取 age name
      val ageAndName: String = tp._1.head
      val movieData: String = if (tp._2.isEmpty) {
        //如果为空 那么2我就要补全数据  用 null补全
        "null null null"
      } else {
        //要对这里的所有的数据进行排序 然后 拼接成字符串
        //按照年份 来排序 就是升序
        val sortedLst: List[String] = tp._2.toList.sortBy(t => t.split(" ")(0).toInt)
        //按照空格把数据拼接成字符串
        sortedLst.mkString(" ")
      }
      ageAndName.concat(",").concat(movieData)
    })
    result.foreach(println)




    sc.stop()


  }
}
