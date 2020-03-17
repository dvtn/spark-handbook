package com.dgqb.spark.core.operation

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionOperationTest")
    val sc = new SparkContext(conf)

    /**
     * reduce
     */

    /**
     * countByKey
     */

    /**
     * countByValue
     */


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
