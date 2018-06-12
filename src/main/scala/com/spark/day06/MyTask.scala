package cn.edu360.spark32.day06

import scala.collection.mutable

/**
  * Created by Huge
  * DATE: 2018/6/10
  * Desc: 
  */

class MyTask extends Serializable{

  // 定义了数据
  val mp = mutable.HashMap[String,Int]("hadoop"-> 100,"spark"->1)
}
