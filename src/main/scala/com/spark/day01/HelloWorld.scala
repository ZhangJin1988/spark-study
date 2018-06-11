package com.spark.day01

/**
  * @author zhangjin
  * @create 2018-06-10 19:43
  */
object HelloWorld {


  def add(x: Int, y: Int) = {
     x + y
  }


  def main(args: Array[String]): Unit = {
    println("hello world")

    //    String[] stri
    // ngs = new String[]{"reba","baba","ruhua"};

    val arr = Array("a", "b", "c")

    for (i <- arr)
      println(i)


    val index = Array(3, 1, 2)

    //    for(i <- index)
    //      println(arr(i))

    for (i <- 1 to 6) {
      println(i)
    }

  }

}
