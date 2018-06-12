package cn.edu360.spark32.day06

/**
  * Created by Huge
  * Desc: 
  */

object UrlUtils {

  def main(args: Array[String]): Unit = {
    println(url2MD5("cast.org.cn").substring(0, 14))
    println(url2MD5("cast.org.cn"))
    println(md5Encoding("cast.org.cn"))
  }


  import java.security.MessageDigest

  def md5Encoding(password: String):String = { // 得到一个信息摘要器
    val digest = MessageDigest.getInstance("md5")
    val result = digest.digest(password.getBytes)
    val buffer = new StringBuffer
    // 把没一个byte 做一个与运算 0xff;
    for (b <- result) { // 与运算
      val number = b & 0xff
      val str = Integer.toHexString(number)
      if (str.length == 1) buffer.append("0")
      buffer.append(str)
    }
    // 标准的md5加密后的结果
    buffer.toString
  }


  // 把url转换为 md5加密的
  def url2MD5(url: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    md5.update(url.getBytes())
    //加密后的字符串
    val digest = md5.digest()
    // 把字节数组转换为16进制
    toHexString(digest)
  }

  //转16进制
  def toHexString(b: Array[Byte]) = {
    val hexChar = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')
    val sb = new StringBuilder(b.length * 2)
    var i = 0
    while (i < b.length) {
      {
        sb.append(hexChar((b(i) & 0xf0) >>> 4))
        sb.append(hexChar(b(i) & 0x0f))
      }
      {
        i += 1
        i - 1
      }
    }
    sb.toString
  }
}
