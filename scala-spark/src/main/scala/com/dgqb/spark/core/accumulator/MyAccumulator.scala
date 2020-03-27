package com.dgqb.spark.core.accumulator

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
 * 累回器(Accumulator)
 *
 * 相当于集群中的统筹大变量
 * 注意：
 *  1.累加器只能在Driver端定义初始化，不能在Executor端定义初始化
 *  2.累加器取值accumulator.value只能在Driver端执行,不能在Executor端.value读值
 *
 *
 */

object MyAccumulator {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local").setAppName("MyAccumulator")
//    val sc = new SparkContext(conf)
//    val rdd1 = sc.textFile("./scala-spark/data/words")
//    var i = 0
//    val rdd2 = rdd1.map(line=>{
//      i += 1
//      line
//    })
//    rdd2.collect()
//    println("i = " + i) //返回0

    val conf = new SparkConf().setMaster("local").setAppName("MyAccumulator")
    val sc = new SparkContext(conf)

    //定义累加器
    val accumulator: Accumulator[Int] = sc.accumulator(0)
    val rdd1 = sc.textFile("./scala-spark/data/words")
    val rdd2 = rdd1.map(line=>{
      accumulator.add(1)
//      println("executor accumulator value ="+accumulator.value) // Can't read accumulator value in task
      println(accumulator)
      line
    })
    rdd2.collect()
    println("accumulator = " + accumulator.value) //返回0

  }
}
