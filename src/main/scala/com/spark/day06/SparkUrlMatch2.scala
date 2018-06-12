package cn.edu360.spark32.day06

import com.spark.utils.MySparkUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Huge
  * DATE: 2018/6/10
  * Desc:
  * 文件url.db, 文件中数据格式为：MD5(url不含http://)+type,  // 截取 14  2
  * 文件40690.txt，文件中数据格式为：url + type //  切分数据 url加密
  * Q1: 40960文件中的url地址加密过后，如果跟url.db文件中的相同，则将url.db中的type更新为40690中的type, 不同的话不做任何修改；
  *
  */

object SparkUrlMatch2 {

  def main(args: Array[String]): Unit = {

    val sc = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    // 加载两个文件的数据
    val urlData = sc.textFile("f:/mrdata/sparkurldata/40690.txt")
    val md5Data = sc.textFile("f:/mrdata/sparkurldata/url.db1000")

    // 对原始文件进行截取 加密 封装成元组
    val urlMd5End: RDD[(String, Int)] = urlData.map(str => {
      //      str  http://01-800.cn	#	12
      val split = str.split("\t#\t")
      val fullUrl = split(0)
      val OldType = split(1).toInt
      // 因为md5加密的数据，没有http:// 需要对数据进行切分
      val url = fullUrl.split("//")(1)

      // 直接调用 md5加密方法对url进行加密
      (UrlUtils.md5Encoding(url).substring(0, 14), OldType)
    })

    // 对加密之后的url进行解析  切分出 url 和 类型
    val md5Split: RDD[(String, Int)] = md5Data.map(str => {
      val md5Url = str.substring(0, 14)
      val newType = str.substring(14).toInt
      (md5Url, newType)
    })

    // 把数据收集为 map
    val broadcast: Broadcast[collection.Map[String, Int]] = sc.broadcast(md5Split.collectAsMap())

    val finalResult: RDD[(String, Int)] = urlMd5End.map(str => {
      val newMd5 = broadcast.value
      val newType = newMd5.getOrElse(str._1, str._2)
      (str._1, newType)
    })

    finalResult.foreach(println)
    sc.stop()
  }
}
