package com.dgqb.spark.core.operation

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionOperationTest")
    val sc = new SparkContext(conf)

//    /**
//     * reduce
//     */
//    val rdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10), 3);
//    val result: Int = rdd.reduce((v1,v2)=>{v1+v2})
//    println(result)



    /**
     * countByKey
     * 只适用于KV格式
     *
     * (金庸,13)
     * (古龙,8)
     */
    val rdd = sc.parallelize(Array(
      ("金庸","飞狐外传"),
      ("金庸","雪山飞弧"),
      ("金庸","连成诀"),
      ("金庸","天龙八部"),
      ("金庸","白马啸西风"),
      ("金庸","鹿鼎记"),
      ("金庸","笑傲江湖"),
      ("金庸","书剑恩仇录"),
      ("金庸","神雕侠女"),
      ("金庸","侠客行"),
      ("金庸","倚天屠龙记"),
      ("金庸","碧血剑"),
      ("金庸","鸳鸯刀"),
      ("古龙","小李飞刀"),
      ("古龙","楚留香传奇"),
      ("古龙","陆小凤传奇"),
      ("古龙","浣花洗剑录"),
      ("古龙","武林外史"),
      ("古龙","萧十一郎"),
      ("古龙","绝代双骄"),
      ("古龙","流星·蝴蝶·剑")

    ), 3);
    val result: collection.Map[String, Long] = rdd.countByKey()
    result.foreach(println)

//    /**
//     * countByValue
//     *
//     * ((古龙,1),6)
//     * ((古龙,2),2)
//     * ((金庸,300),4)
//     * ((金庸,100),7)
//     * ((金庸,200),2)
//     */
//    val rdd = sc.parallelize(Array(
//      ("金庸","100"),
//      ("金庸","100"),
//      ("金庸","100"),
//      ("金庸","100"),
//      ("金庸","100"),
//      ("金庸","100"),
//      ("金庸","100"),
//      ("金庸","200"),
//      ("金庸","200"),
//      ("金庸","300"),
//      ("金庸","300"),
//      ("金庸","300"),
//      ("金庸","300"),
//      ("古龙","1"),
//      ("古龙","1"),
//      ("古龙","1"),
//      ("古龙","1"),
//      ("古龙","1"),
//      ("古龙","1"),
//      ("古龙","2"),
//      ("古龙","2")
//
//    ), 3);
//    rdd.countByValue().foreach(println)


    //    /**
//     * foreachPartiton: 一个分区一个分区的处理数据
//     */
//    val rdd = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",40),("张三丰",80),("独孤求败",60),("风清扬",40),("张三丰",80)),3)
//    rdd.foreachPartition(iter=>{
//      iter.foreach(println)
//    })

    //    val lines = sc.textFile("./scala-spark/data/words")
//
//    /**
//     * count:返回结果计数,将结果回收到Driver端
//     */
//    val result = lines.count()
//    println("count:"+result)
//
//
//    /**
//     * collect(): Driver回收结果到Driver端
//     */
//    val array: Array[String] = lines.collect()
//    array.foreach(println)
//
//    /**
//     * first():取结果第一行
//     */
//    val firstLine: String = lines.first()
//    println(firstLine)
//
//    /**
//     * take(num): 返回结果的前5行
//     * take(1) == first()
//     */
//    val take: Array[String] = lines.take(5)
//    take.foreach(println)

    sc.stop()
  }

}
