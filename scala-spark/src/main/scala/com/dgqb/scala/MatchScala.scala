package com.dgqb.scala

/**
 * 模式匹配
 * 1."_=>"模式匹配，放在最后，相当于java中的default
 * 2.模式匹配不仅可以匹配值还可以匹配类型
 * 3.从上往下匹配，切配上一个之后就不会继续向下匹配,匹配过程中有类型自动转换
 */

object MatchScala {


  def MatchTest(o: Any): Unit = {
    o match {
      case 1=>println("value is 1")
      case i:Int => println("type is Int") //i当前值过来的一个代号,可以是任意名称
      case s:String => println("type is String")
      case "hello"=> println("value is hello")
      case d:Double => println("type is double")
      case _=> println("default...")
    }
  }

  def main(args: Array[String]): Unit = {
     val tuple = (1,"hello",'c',1.0,1.1,true)
     val it = tuple.productIterator
     while(it.hasNext){
       val one = it.next()
       MatchTest(one)
     }
  }

}
