package com.dgqb.scala

import java.util.Date


object FunctionScala {
  def main(args: Array[String]): Unit = {
    /**
     * 1.方法定义
     *    1.return可以省略，省略之后，方法体中最后一行计算的结果当做返回值返回，return如果写，方法的返回值类型不能省略。
     *    2.方法的返回值类型可以省略，会自动推断。
     *    3.定义方法用"def"，方法的参数不能省略类型。
     *    4.如果方法体可以一行搞定，那么方法的方法体"{}"可以省略不写。
     *    5.定义方法"="可以省略，省略了方法的返回值为Unit, 无论方法最后一行计算结果是什么都会被丢弃，返回Unit
     */
    def max1(a: Int, b: Int) = {
      if (a > b) {
        a
      } else {
        b
      }
    }

    def max2(a: Int, b: Int) = if (a > b) a else b

    /**
     * 递归函数
     * 递归函数要显式定义函数的返回值类型
     */
    def fibonacci(num: Int): Int = {
      if (num <= 2) {
        1
      } else {
        fibonacci(num - 1) + fibonacci(num - 2)
      }
    }

    println(fibonacci(10))

    /**
     * 3.有默认值的函数
     * 可以使用位置参数
     */
    def f3(a: Int = 10, b: Int = 100) = {
      a + b
    }

    println(f3())
    println(f3(a = 3))
    println(f3(b = 3))

    /**
     * 4.可变长参数的函数
     */
    def f4(elems: String*) = {
      elems.foreach(println)
    }

    f4("a", "b", "c", "Hello")


    /**
     * 5.匿名函数
     *
     * ()=>{}
     */
    val fun = (a: Int, b: Int) => {
      println("Hello World" + a + ":" + b)
    }

    fun(1, 2)

    val fun1 = (a: Int, b: Int) => {
      a + b
    }

    println(fun1(2, 3))


    /**
     * 6.嵌套函数
     */
    def f6(a: Int) = {
      def fun1(num: Int): Int = {
        if (num == 1) {
          1
        } else {
          num * fun1(num - 1)
        }
      }
      fun1(a)
    }

    println(f6(5))

    /**
     * 7.偏应用函数
     * web用的比较多
     * 偏应用函数是一个表达式，将方法中不变的参数写上，变化的参数用"_"表示.下次直接调用这个偏应用表达式直接传入变化的参数就可以。
     */
    def printLog(d: Date, log:String) = {
      println("The date is: "+d+", the log is: "+log)
    }

    val date = new Date()
    printLog(date, "aaa")
    printLog(date, "bbb")
    printLog(date, "ccc")
    //相保持上面的date不变,应用偏应用函数，变的用"_"表示
    val fun7=printLog(date:Date,_:String)
    fun7("123")
    fun7("456")
    fun7("789")

    /**
     * 8.高阶函数
     *    1.函数的参数是函数
     *    2.函数的返回是函数
     *      函数的返回是函数时，要显式声明函数类型
     *    3.函数的参数和返回都是函数
     */
    //函数的参数是函数
    def fun8(a:Int,b:Int):Int = {
      a+b
    }

    def f81(str:String, f:(Int,Int)=>Int) = {
      val result = f(10,20)
      str+"~"+result
    }

    println(f81("Hello World",fun8))

    //匿名函数传入
    val result = f81("您好",(a:Int,b:Int)=>{a*b})
    println(result)

    //函数的返回是函数
    def f82(a:Int, b:Int):(String,String)=>String = {
      val result = a*b
      def fun1(s:String, s1:String):String = {
        s+"@"+s1+"#"+result
      }
      fun1
    }

    println(f82(10,20)("您好","中国"))

    //函数的参数和返回都是函数
    def f83(f:(Int,Int)=>Int):(String,String)=>String = {
      val result = f(5,6)
      def fun1(s1:String, s2:String):String = {
        s1+"@"+s2+"#"+result
      }
      fun1
    }

    println(f83((a:Int,b:Int)=>{a*b})("Hello", "World"))

    /**
     * 9.柯里化函数
     * 柯里化函数就是高阶函数的简化版
     *
     */
    def f90(a:Int, b:Int)(c:Int,d:Int) ={
      a+b+c+d
    }

    println(f90(1,2)(3,4))

  }
}
