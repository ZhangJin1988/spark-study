package com.spark.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/*
http://bigdata.edu360.cn/laozhang
网站的访问日志，最后面是老师的名称。
javaee
bigdata
php
代表着各个子学科。

要求统计全局的TopN和分组（分学科的）的TopN。  过滤

注意事项：
1，要把学科和老师一起当成key。
2,注意切分使用的特殊分隔符需要进行转义。 [.]  \\.

 */
object TeacherCount {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    //
    val sc: SparkContext = new SparkContext(conf)

    val datas: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/teacher.log")

    //第一步 拿到拆分后的原始数据 封装到RDD分布式集合中

    //http://bigdata.edu360.cn/laozhang

    val tuples: RDD[(String, String)] = datas.map(line => {
      val splits: Array[String] = line.split("//")
      val teacher = splits(1).split("/")(1)
      val project = splits(1).split("\\.")(0)
      (project, teacher)
    })
    //  学科 老师
    //    tuples.foreach(println)

    //拿到老师分组的数据 然后累加
    val teachers: RDD[(String, Iterable[(String, String)])] = tuples.groupBy(_._2)

    val teacherTopN: RDD[(String, Int)] = teachers.map(kv => (kv._1, kv._2.size)).sortBy(_._2)
    teacherTopN.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-study/output5")


    //拿到按学科分组的topN数据
    //思路 先按照 学科分组 然后 单个学科类 按照老师分组 再累加


    val projects: RDD[(String, Iterable[(String, String)])] = tuples.groupBy(_._1)

    val projectTopN: RDD[(String, String, Int)] = projects.flatMap(kv => {
      //按照老师分组
      val teachers: List[(String, String, Int)] = kv._2.groupBy(_._2).toList.map(tp => (kv._1, tp._1, tp._2.size)).sortBy(_._3)
      teachers
    })
    projectTopN.saveAsTextFile("/Users/zhangjin/myCode/learn/spark-study/output6")


  }

}
