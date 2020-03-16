package com.dgqb.spark.core.operation

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionOperationTest")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("./scala-spark/data/words")

    /**
     * count:返回结果计数,将结果回收到Driver端
     */
    val result = lines.count()
    println("count:"+result)


    /**
     * collect(): Driver回收结果到Driver端
     */
    val array: Array[String] = lines.collect()
    array.foreach(println)

    /**
     * first():取结果第一行
     */
    val firstLine: String = lines.first()
    println(firstLine)

    /**
     * take(num): 返回结果的前5行
     * take(1) == first()
     */
    val take: Array[String] = lines.take(5)
    take.foreach(println)

    sc.stop()
  }

}
