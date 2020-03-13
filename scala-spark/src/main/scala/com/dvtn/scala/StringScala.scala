package com.dvtn.scala
/**
 * 其实就是java中的String
 */

object StringScala {
  def main(args: Array[String]): Unit = {

    val s1 = "hello world"
    val s2 = "HELLO WORLD"

    println(s1.equals(s2)) //false
    println(s1.equalsIgnoreCase(s2))  //true

    val sb = new StringBuilder()
    sb.+('a')
    sb.++=("hello")
    sb.append(true)
    println(sb)

  }
}
