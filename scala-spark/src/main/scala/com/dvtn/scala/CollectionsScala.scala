package com.dvtn.scala

object CollectionsScala {
  def main(args: Array[String]): Unit = {
    /**
     * 定义Array
     */

    val arr = Array(1,2,3,4,"hello",'c', true)

    val arr1 = Array[Int](1,2,3,4)
    //遍历
    arr.foreach(println)

    for(elem <- arr){
      println(elem)
    }

    //小括号里代表数组的长度
    val arr2 = new Array[String](3)
    arr2(0) = "Hello"
    arr2(1) = "World"
    arr2(2) = "Scala"
    arr2.foreach(println)


    /**
     * List:有序可重复
     */
    val list = List[Int](1,2,3,4,1,2,3,4,1,1,2,2,2,2)
    //遍历
    list.foreach(println)
    //遍历
    for(elem <- list){
      println(elem)
    }
    val list1 = List("hello world","hello java","hello scala")

    //map
    val result = list1.map(s=>{
      s.split(" ")
    })
    result.foreach(arr => {
      println("*************")
      arr.foreach(println)
    })

    //flatMap
    val result1 = list1.flatMap(s=>{
      s.split(" ")
    })
    result1.foreach(println)

    //filter
    val result2 = list1.filter(s=>{
      !s.contains("java")
    })
    println(result2)


    /**
     * Set: 无序不重复
     */
    val set = Set[Int](1,2,2,3,4,5,5,6,6,6)
    set.foreach(println)

    /**
     * Map:
     */
    val map = Map(1->"风清扬", 2->"独孤求败", (3,"林青霞"),(3,"郭靖"))
    //元素是一个个的二元组
    map.foreach(println)
    //取值
    val v1 = map.get(2) //如果有值返回Some类型
    val v2 = map.get(4) //返回None
    val v3 = map.getOrElse(4,"杨过")
    println(v1)
    println(v2)
    println(v3)
//    val v4 = map.get(4).get //如果键不存在，报错
    val v5 = map.get(5).getOrElse("xxxx")//如果不存在返回默认值
//    println(v4)
    println(v5)

    //返回所有的键
    val keys = map.keys
    for(key <- keys){
      println(key)
    }
    //返回所有的values
    val values = map.values
    values.foreach(println)

    /**
     * Tuple 元组
     * tuple最多支持22个元素Tuple1 ~ Tuple22
     */
    val tuple = new Tuple5(1,2,3,4,"hello")
    val tuple1 = Tuple6(1,2,3,4,5,6)
    val tuple2 = (1,2,3,4,5,6)

    //tuple遍历
    val it = tuple.productIterator
    it.foreach(println)

    while(it.hasNext){
      println(it.next())
    }

    val tuple3 = ("name","风清扬")
    println(tuple3.toString())
    println(tuple3._2)

    //swap方法,只能用于Tuple2
    println(tuple3.swap._1)



  }
}
