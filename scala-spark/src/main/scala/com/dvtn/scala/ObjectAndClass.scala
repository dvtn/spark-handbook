package com.dvtn.scala

/**
 * 1.scala中定义在object中的变量，方法都是静态的。object叫对象，相当于java中的单例对象。object不可以传参。Trait也不可以传参。
 * 2.scala中一行代码后可以写";"，也可以不写，会有分号推断机制。多行代码写在一行要用分号隔开;
 * 3.定义变量用var, 定义常量用val. a:Int ":Int"是变量的类型，可以写也可以不写；
 * 4.scala中的变量，类，对象，方法定义建议符合驼峰命名法；
 * 5.class是scala中的类。类可以传参，传参就有了默认的构造函数。类中的属性默认有getter, var类型的属性有setter和getter
 * 6.重写构造，第一行要调用默认的构造.
 * 7.class中当new一个对象时，类中除了方法不执行，其它都执行。同一个包下，class名称不能相同。
 * 8.scala中如果一个class名称与object名称一致，好么这个class叫做这个object的伴生类，object叫做这个class的伴生对象,它们可以互相访问私有变量
 *
 *
 *
 *
 *
 *
 *
 */

//scala中的类可以传参,参数要定义类型
class Person(xname: String, xage: Int) {
  val name = xname //默认修饰符是public的,也可以用private修饰
  val age = xage
  var gender = '男' //默认值男

  /**
   * class中当new一个对象时，类中除了方法不执行，其它都执行
   */
  println("*************************************")

  //重写构造方法
  def this(yname: String, yage: Int, ygender: Char) {
    this(yname, yage)
    this.gender = ygender
  }

  def showName() = {
    println("Hello World")
  }
}

object ObjectAndClass {
  /**
   * 静态的语句先加载
   */
  println("#####################################")

  def main(args: Array[String]): Unit = {
    /**
     * 类
     */
    val person = new Person("林青霞", 28)
    //    println(person.name)
    //    println(person.age)

    //    val p1 = new Person("独孤求败", 30, '男')
    //    println(p1.name + p1.age + p1.gender)
    //    p1.showName()

    /**
     * 变量 常量
     */
    //    var a = 100;
    //    println(a)
    //    val b = 100
    //    println(b)

  }
}
