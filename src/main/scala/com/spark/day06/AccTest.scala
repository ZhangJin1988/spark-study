package cn.edu360.spark32.day06

import com.spark.utils.MySparkUtils
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable

/**
  * Created by Huge
  * DATE: 2018/6/10
  * Desc: 
  */

object AccTest {

  def main(args: Array[String]): Unit = {
    val mp = new mutable.HashMap[Int, Long]()

    val sc = MySparkUtils.getLocalSparkContext(this.getClass.getSimpleName)

    val rdd1 = sc.makeRDD(List(1, 3, 43, 5, 65), 2).zipWithIndex()
    var cnts = 0

    // 累加器  都是 唯一的

    // 设置了一个名称为   初始值为0 的累加器
    val acc = sc.accumulator(0, "cntsss") //spark  1.x

    // spark 2.x 版本
    val acc2: LongAccumulator = sc.longAccumulator("abc")

    rdd1.foreach(t => {
      mp(t._1) = t._2
      cnts += t._1
      //      println(s"for---mp = ${mp.size}")
      //      println(s"for---cnts = ${cnts}")

      // 统计次数
      acc.add(1)
      acc2.add(1L)
    })


    //    println(mp.size)
    //    println("cnts = " + cnts)

    // 在driver端统计 累加器的值
    println(acc.value)
    println(acc2.value)

    sc.stop()
  }
}
