package com.dgqb.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 广播变量
 *  1.不能将RDD广播出去,可以将RDD的结果广播出去
 *  2.广播变量只能在Driver端定义，在Executor端使用，不能在Executor端改变
 *  3.如果不使用广播变量，在一个Executor中有多少task,就有多少变量副本；
 *    如果使用广播变量，在每个Executor中只有一份Driver端的变量副本
 */

object MyBroadcast {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("MyBroadcast")
    val sc = new SparkContext(conf)
    val blacklist: List[String] = List[String]("风清扬","任我行")
    val bcList: Broadcast[List[String]] = sc.broadcast(blacklist) //使用广播变量
    val rdd1: RDD[String] = sc.parallelize(Array(
      "风清扬","任我行","独孤求败","东方不败","成坤","张三丰",
      "风清扬","任我行","独孤求败","东方不败","成坤","张三丰",
      "风清扬","任我行","独孤求败","东方不败","成坤","张三丰"
    ))
    val rdd2: RDD[String] = rdd1.filter(s=>{
      val list: List[String] = bcList.value
      !list.contains(s)
    })
    rdd2.foreach(println)

    sc.stop()

  }
}
