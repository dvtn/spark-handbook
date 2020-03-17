package com.dgqb.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Spark Pipeline处理数据
 */
object SparkPipeline {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SparkPipelineTest")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.parallelize(Array("独孤求败", "风清扬", "任我行", "东方不败"))
    val rdd2: RDD[String] = rdd1.map(name => {
      println("##########map##########" + name)
      name + "@"
    })

    val rdd3: RDD[String] = rdd2.filter(name => {
      println("==========filter=======" + name)
      true
    })

    rdd3.collect()


  }
}
