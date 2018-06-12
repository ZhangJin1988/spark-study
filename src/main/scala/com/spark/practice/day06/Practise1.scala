package com.spark.practice.day06

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import cn.edu360.spark32.day06.IPLocalAcc.ip2Long
import com.spark.utils.{IpUtils, MySparkUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * @author zhangjin
  * @create 2018-06-11 20:06
  */
object Practise1 {

  /**
    * orders.log是一个电商网站某一天用户购买商品的订单成交数据，每一行有多个字段，用空格分割，字段的含义如下
    * 用户ID   ip地址          商品分类   购买明细     商品金额
    * A        202.106.196.115 手机       iPhone8      8000
    *
    * A 202.106.196.115 手机 iPhone8 8000
    *
    * 问题1.计算出各个省份的成交总额（结果保存到MySQL中）
    * 问题2.计算每个省城市成交量的top3（结果保存到MySQL中）
    * 问题3.计算每个商品分类的成交总额，并按照从高到低排序（结果保存到MySQL中）
    * 问题4.构建每一个用户的用户画像，就是根据用户购买的具体商品，给用户打上一个标签，为将来的商品推荐系统作数据支撑
    *
    * 说明：如果一个用户购买了一个iPhone8，对应有多个标签：果粉、高端人士、数码一族
    * 请将下面的规则数据保存到MySQL数据库中，并作为标签规则(三个字段分别代表id、商品、对应的标签)：
    *
    * 1 iPhone8 果粉
    * 2 iPhone8 高端人士
    * 3 iPhone8 数码一族
    * 4 布莱奥尼西服 高端人士
    * 5 布莱奥尼西服 商务男士
    * 6 婴儿床 育儿中
    * 7 迪奥香水 高端人士
    * 8 迪奥香水 白富美
    * 9 婴儿车 育儿中
    * 10 iPhone8手机壳 果粉
    * 11 iPhone8手机壳 高端人士
    * 12 iPhone8手机壳 数码一族
    * 13 spark实战 IT人士
    * 14 spark实战 屌丝
    * 15 Hadoop编程指南 IT人士
    * 16 Hadoop编程指南 人工智能专家
    * 17 奶粉 育儿中
    *
    * @param args
    */


  def main(args: Array[String]): Unit = {


    val sc: SparkContext = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)


    //拿到Ip的数据 可以根据Ip获取其他的信息
    val ipData: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/input/ip.txt")

    val ipRulesProvinceAndCityRdd: RDD[(Long, Long, String, String)] = ipData.map(log => {
      // 起始ip的10进制  终止ip的10进制  归属地
      val split = log.split("\\|")
      val start = split(2).toLong
      val end = split(3).toLong
      val province = split(6)
      val city = split(7)
      // 返回一个元组
      (start, end, province, city)
    })
    val ipRules: Array[(Long, Long, String, String)] = ipRulesProvinceAndCityRdd.collect()



    //获取原始的截取数据

    val ordersRdd: RDD[String] = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/input/orders.log")
    val datas: RDD[(String, String, String, String, Double, String)] = ordersRdd.map(order => {

      val split: Array[String] = order.split(" ")
      val ip = split(1)

      val provinceAndCity: (String, String) = IpUtils.binarySearchProvinceAndCity(IpUtils.ip2Long(ip), ipRules)
      var province = provinceAndCity._1
      var city = provinceAndCity._2
      val productType = split(2)
      val productName = split(3)
      val price = split(4).toDouble
      val user = split(0)
      (province, city, productType, productName, price, user)
    })
    datas.foreach(println)


    //第一题 统计各个身份的成交额
    val groupProvince: RDD[(String, Iterable[(String, String, String, String, Double, String)])] = datas.groupBy(_._1)
    val result1: RDD[(String, Double)] = groupProvince.map(kv => {
      val totalPrice: Double = kv._2.map(tp => tp._5).sum
      (kv._1, totalPrice)
    }).sortBy(_._2)
    result1.foreach(println)


    // 把结果数据 写入到mysql中

    result1.foreach(tp => {

      var conn: Connection = null
      var pstm: PreparedStatement = null

      try {
        val url = "jdbc:mysql://hdp1:3306/scott?characterEncoding=utf-8"
        conn = DriverManager.getConnection(url, "root", "Zj314159!")

        val sql = "insert into order_1 values(?,?)"
        pstm = conn.prepareStatement(sql)

        // 赋值
        pstm.setString(1, tp._1)
        pstm.setDouble(2, tp._2)

        pstm.execute()

      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        if (pstm != null) pstm.close()
        if (conn != null) conn.close()
      }
    })




    //第二题  问题2.计算每个省城市成交量的top3（结果保存到MySQL中）

    //分析  第一 按照 省份  城市 分组 聚合  获取各个城市的成交额   然后 再按照 省份分组  取top3  排序插入

    val groupProvinceAndCity: RDD[(String, Iterable[(String, String, String, String, Double, String)])] = datas.groupBy(_._2)
    val result2: RDD[(String, String, Double)] = groupProvinceAndCity.map(kv => {
      val totalPrice: Double = kv._2.map(tp => tp._5).sum
      val province = kv._2.toList(0)._1
      (province, kv._1, totalPrice)
    }).sortBy(_._3)
    val dataRdd: RDD[(String, Iterable[(String, String, Double)])] = result2.groupBy(_._1)

    val result3: RDD[(String, List[(String, String, Double)])] = dataRdd.mapValues(v => {
      val results: List[(String, String, Double)] = v.toList.sortBy(_._3).take(3)
      results
    })
    // 把结果数据 写入到mysql中

    result3.foreach(tp => {

      var conn: Connection = null
      var pstm: PreparedStatement = null

      try {
        val url = "jdbc:mysql://hdp1:3306/scott?characterEncoding=utf-8"
        conn = DriverManager.getConnection(url, "root", "Zj314159!")

        val sql = "insert into order_2 values(?,?,?)"
        pstm = conn.prepareStatement(sql)

        for (i <- tp._2) {
          // 赋值
          pstm.setString(1, tp._1)
          pstm.setString(2, i._2)
          pstm.setDouble(3, i._3)

          pstm.execute()
        }


      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        if (pstm != null) pstm.close()
        if (conn != null) conn.close()
      }
    }) //    result2.foreach(println)


    //第三题   问题3.计算每个商品分类的成交总额，并按照从高到低排序（结果保存到MySQL中）

    val groupProductType: RDD[(String, Iterable[(String, String, String, String, Double, String)])] = datas.groupBy(_._3)
    val result4: RDD[(String, Double)] = groupProductType.map(kv => {
      val totalPrice: Double = kv._2.map(tp => tp._5).sum
      (kv._1, totalPrice)
    }).sortBy(_._2)
    result4.foreach(println)


    result4.foreach(tp => {

      var conn: Connection = null
      var pstm: PreparedStatement = null

      try {
        val url = "jdbc:mysql://hdp1:3306/scott?characterEncoding=utf-8"
        conn = DriverManager.getConnection(url, "root", "Zj314159!")

        val sql = "insert into order_3 values(?,?)"
        pstm = conn.prepareStatement(sql)

        //        for (i <- tp._2) {
        // 赋值
        pstm.setString(1, tp._1)
        pstm.setDouble(2, tp._2)

        pstm.execute()
        //        }


      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        if (pstm != null) pstm.close()
        if (conn != null) conn.close()
      }
    }) //


    //    * 问题4.构建每一个用户的用户画像，就是根据用户购买的具体商品，给用户打上一个标签，为将来的商品推荐系统作数据支撑

    //1  思路 保存标签规则表
    //后台操作  搞定任务
    // 2 然后 按照用户分组  获取 用户购买的商品列表

    val userProducts: RDD[(String, Iterable[(String, String, String, String, Double, String)])] = datas.groupBy(_._6)
    val productsData: RDD[(String, List[String])] = userProducts.mapValues(v => {
      val products = v.map(tp => tp._4)
      products.toList
    })


    // 3根据商品遍历 查询标签
    productsData.foreach(tp => {

      var conn: Connection = null
      var pstm: PreparedStatement = null

      try {
        val url = "jdbc:mysql://hdp1:3306/scott?characterEncoding=utf-8"
        conn = DriverManager.getConnection(url, "root", "Zj314159!")

        var uesType: mutable.HashSet[String] = new mutable.HashSet
        tp._2.foreach(v => {
          //查询标签
          val sql1 = "select user_type,produce_name,id from user_type_rule where produce_name = ?"
          pstm = conn.prepareStatement(sql1)
          pstm.setString(1, v)
          val set: ResultSet = pstm.executeQuery()
          if (set != null) {
            while (set.next()) {
              println(v + "---------")
              println(set.getString("user_type") + set.getString("produce_name") + set.getInt("id"))
              //              uesType.+(set.getString("user_type" ))


              val sql = "insert into user_type_log values(?,?)"
              pstm = conn.prepareStatement(sql)


              //        for (i <- tp._2) {
              // 赋值
              pstm.setString(1, tp._1)
              pstm.setString(2, set.getString("user_type"))

              pstm.execute()
            }
          }
        })
        //        val types: String = uesType.mkString(" ")

        //        println()


        //        val sql = "insert into user_type_log values(?,?)"
        //        pstm = conn.prepareStatement(sql)


        //        for (i <- tp._2) {
        // 赋值
        //          pstm.setString(1, tp._1)
        //        pstm.setString(2, types)

        pstm.execute()
        //        }


      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        if (pstm != null) pstm.close()
        if (conn != null) conn.close()
      }
    }) //

    // 4 最后 拼接所有的标签 保存到数据库


    sc.stop()


  }

}
