package cn.edu360.spark32.day06

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

/**
  * Created by Huge
  * DATE: 2018/6/10
  * Desc: 
  */

object SerDemo {

  def main(args: Array[String]): Unit = {
    val p1 = new Person("nianhang", 28)
    // 把内存中的对象写入到文件中
    /*val oos = new ObjectOutputStream(new FileOutputStream("f:/mrdata/person.txt"))


    oos.writeObject(p1)
    oos.writeInt(100)
    oos.writeUTF("xx00")*/

    val oos = new ObjectInputStream(new FileInputStream("f:/mrdata/person.txt"))

    val ro: AnyRef = oos.readObject()
    val p2 = ro.asInstanceOf[Person]
    println(p2)
    println(p1 == p2)

    println(oos.readInt())
    println(oos.readUTF())
    oos.close()
  }
}


class Person(val name: String, val age: Int) extends Serializable{

  override def toString = s"Person($name, $age)"
}