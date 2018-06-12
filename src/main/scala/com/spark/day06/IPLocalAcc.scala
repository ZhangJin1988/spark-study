package cn.edu360.spark32.day06

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.spark.utils.MySparkUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Huge
  * DATE: 2018/6/9
  * Desc: 
  * 根据ip地址计算归属地，然后把数据写入到mysql中
  *
  */

object IPLocalAcc {

  // 定义一个方法，把ip地址转换为10进制
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  // 定义一个二分搜索的方法
  def binarySearch(longIp: Long, ipRules: Array[(Long, Long, String)]): String = {
    // 定义两个索引
    var low = 0
    var high = ipRules.length - 1

    // 只要满足条件，就进行二分搜索
    while (low <= high) {
      //中间值的索引
      val middle = (low + high) / 2
      // 接收 middle位置的值
      val (start, end, province) = ipRules(middle)
      if (longIp >= start && longIp <= end) {
        // 找到了，就直接返回省份值
        // 利用return关键字返回省份结果
        return province
      } else if (longIp < start) { // 要查找的值在左区间
        // 缩小查找范围，把high 调小
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    // 如果程序运行到这里，没有找到结果
    "unknown"
  }


  def main(args: Array[String]): Unit = {
    val sc: SparkContext = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    // 统计 转10进制的时间      写入到mysql的时间
    val ip2LongTime = sc.accumulator(0L, "ip2LongTime")
    val ip2MySql = sc.accumulator(0L, "ip2MySql")


    // 读取数据
    val logrdd: RDD[String] = sc.textFile("f:mrdata/ipdata/ipaccess.log")

    val ipData: RDD[String] = sc.textFile("f:/mrdata/ipdata/ip.txt")

    // 对两个数据集进行 数据的切分 提取有效数据

    // 对日志数据中的所有的ip地址，转换为10进制的ip
    val longIpRdd: RDD[Long] = logrdd.map(log => {
      val t1 = System.currentTimeMillis()
      val split = log.split("\\|")
      // 获取到了ip字段
      val ipstr: String = split(1)

      // 把字符串ip转换成 10进制的ip
      val long = ip2Long(ipstr)

      val t2 = System.currentTimeMillis()

      ip2LongTime.add(t2 - t1)

      long
    })

    val ipRulesRdd: RDD[(Long, Long, String)] = ipData.map(log => {
      // 起始ip的10进制  终止ip的10进制  归属地
      val split = log.split("\\|")
      val start = split(2).toLong
      val end = split(3).toLong
      val province = split(6)
      // 返回一个元组
      (start, end, province)
    })

    // 如何比较数据

    //    // 不能在一个rdd中操作另一个rdd
    //    longIpRdd.map(ip =>{
    //      ipRulesRdd.filter(t=>ip>= t._1 && ip <= t._2)
    //    }).foreach(println)

    val ipRules: Array[(Long, Long, String)] = ipRulesRdd.collect()

    val provinceAndOne: RDD[(String, Int)] = longIpRdd.map(ip => {
      // 调用方法 获取到省份值
      val province = binarySearch(ip, ipRules)
      // 把得到的结果和1 组装成元组
      (province, 1)
    })

    // 分组聚合
    val result: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)


    // 把结果数据 写入到mysql中

    result.foreach(tp => {

      var conn: Connection = null
      var pstm: PreparedStatement = null

      try {
        val url = "jdbc:mysql://localhost:3306/scott?characterEncoding=utf-8"
        conn = DriverManager.getConnection(url, "root", "123")

        val sql = "insert into access_log values(?,?)"
        pstm = conn.prepareStatement(sql)

        // 赋值
        pstm.setString(1, tp._1)
        pstm.setInt(2, tp._2)

        pstm.execute()

      } catch {
        case e: Exception => e.printStackTrace()
      }
      finally {
        if (pstm != null) pstm.close()
        if (conn != null) conn.close()
      }
    })

    sc.stop()
  }
}
