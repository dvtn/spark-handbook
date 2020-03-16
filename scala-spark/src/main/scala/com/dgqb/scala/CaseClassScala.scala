package com.dgqb.scala

/**
 * case class: 样例类
 * 使用了case关键字的类定义就是样例类(case class)，样例类是特殊的类。
 * 1.实现了类构造参数的getter方法（构造参数默认被声明为val),当构造参数是声明为var类型时，它将自动实现setter和getter方法。
 * 2.样例类默认帮你实现了toString, equals, copy和hashCode方法。
 * 3.样例类可以new, 也可以不用new
 */

case class Human(name: String, age: Int)

object CaseClassScala {
  def main(args: Array[String]): Unit = {
    val h1 = new Human("独孤求败", 60)

    val h2 = new Human("独孤求败", age = 60)
    println(h1.equals(h2))

    val h3 = Human("风清扬", 50)
  }
}
