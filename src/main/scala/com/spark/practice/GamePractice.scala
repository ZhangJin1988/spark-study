package com.spark.practice

import java.text.SimpleDateFormat
import java.util.Date

import com.spark.utils.MySparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author zhangjin
  * @create 2018-06-06 22:17
  */
object GamePractice {

  def main(args: Array[String]): Unit = {


    //第一步 读取数据

    val sc: SparkContext = MySparkUtils.getLocalSparkContext(this.getClass().getSimpleName)

    val datas: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/input/user.log")

    //第二步  拿到原始数据
    val datasRdd: RDD[(String, Int, String)] = datas.map(k => {
      val strs: Array[String] = k.split("\\|")
      //      var logType: Int = 0
      //      var day: String = "null"
      //      var ip: String = "null"
      //      if(strs.size == 9){
      var logType = strs(0).toInt
      var day = strs(1).split(",")(0)
      var ip = strs(2)
      //      }
      (day, logType, ip)
    })

    //第三步  按天分组 统计日志类型为1 的数据
    val dayNews: RDD[(String, Iterable[(String, Int, String)])] = datasRdd.groupBy(_._1)
    val dayNewsResult: RDD[(String, Int)] = dayNews.mapValues(_.toList.filter(_._2 == 1).size).sortByKey()
    dayNewsResult.foreach(println)

    dayNewsResult.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-study/output8")


    //解答第二题
    //思路   第一步 生成两组数据 第一组正常 过滤新增的用户 第二组 所有的时机 减去1
    //第二步 正常的新增数据 按照 天 和 用户 做内连接  得到一个数据  然后 除以 当天的新增用户数据 就是留存率


    //得到新增用户 和 天 作为key去重后的数据

    val initDatas: RDD[(String, Iterable[(String, Int, String)])] = datasRdd.groupBy(_._1)

    //每天新增的用户数据
    val newAddUserData: RDD[(String, List[(String, Int, String)])] = initDatas.mapValues(_.toList.filter(_._2 == 1))

    val yesterDatas: RDD[(String, List[(String, Int, String)])] = initDatas.map(kv => {
      val date: Date = new SimpleDateFormat("yyyy年MM月dd日").parse(kv._1)
      val yesterDay = new Date(date.getTime - 24 * 60 * 60 * 1000l)
      (new SimpleDateFormat("yyyy年MM月dd日").format(yesterDay), kv._2.toList)
    })
    yesterDatas.foreach(println)
    //    val results: RDD[(String, List[(String, Int, String)])] =
    val groupDatas: RDD[(String, (Iterable[List[(String, Int, String)]], Iterable[List[(String, Int, String)]]))] = newAddUserData.groupWith(yesterDatas)


    val leftResult: RDD[(String, Double, Double, Double)] = groupDatas.map(kv => {
      val lstNew1: List[(String, Int, String)] = kv._2._1.toList.flatten
      val lstYed2: List[(String, Int, String)] = kv._2._2.toList.flatten
      val newAddNum: Double = lstNew1.size.toDouble
      val lst1: List[String] = lstNew1.map(kv => kv._3)
      val lst2: List[String] = lstYed2.map(kv => kv._3)
      val leftDatas: List[String] = lst1.intersect(lst2)
      val leftNum: Double = leftDatas.size.toDouble
      (kv._1, newAddNum, leftNum, leftNum / newAddNum)
    }).sortBy(_._1)
    leftResult.foreach(println)


    leftResult.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-study/output9")



    //    val leftDatas: RDD[(String, Int)] = results.mapValues(_.size)


    //    val

    //    (kv=>kv._2.toList.filter(_._2==1))


  }


}
