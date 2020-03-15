package com.dvtn.spark.core.operation

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 持久化算子：
 *  cache(): 默认将RDD中的数据缓存在内存中，懒执行算子。
 *
 *  persist(): 可以手支指定持久化级别, 懒执行算子，需要action算子触发
 *    persist(StorageLevel.MEMORY_ONLY) = cache() = persist()
 *
 *    StorageLevel 常用：
 *    MEMORY_ONLY
 *    MEMORY_ONLY_SER
 *    MEMORY_AND_DISK
 *    MEMORY_AND_DISK_SER
 *
 *    cache和persist注意点：
 *      1.二者都是懒执行，需要Action算子触发执行。
 *      2.对一个RDD cache或者persist之后可以赋值给一个变量，下次直接使用这个变量就是使用持久化的rdd。
 *      3.如果赋值给一个变量，那么cache和persist之后不能紧跟Action算子
 *
 *
 *    cache, persist, checkpoint 持久化的单位都是partition
 */

object PersistOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("PersistOperation")
    val sc = new SparkContext(conf)

//    var rdd = sc.textFile("./scala-spark/data/orders.csv")



    sc.setCheckpointDir("./scala-spark/checkpoint")
    val rdd1 = sc.textFile("./scala-spark/data/words")
    rdd1.checkpoint()
    rdd1.collect()


//    rdd.cache()
//    rdd.persist(StorageLevel.MEMORY_ONLY)
    //如果赋值给一个变量，那么cache和persist之后不能紧跟Action算子
//    val result = rdd.cache().collect()


//    val startTime = System.currentTimeMillis()
//    val result1: Long = rdd.count()
//    val endTime = System.currentTimeMillis()
//    //从磁盘读数据时间
//    println("count = "+result1+" ,duration = "+(endTime-startTime)+"ms")
//
//
//    val begin = System.currentTimeMillis()
//    val result2: Long = rdd.count()
//    val end = System.currentTimeMillis()
//    //从内存读取数据时间
//    println("count = "+result1+" ,duration = "+(end-begin)+"ms")

    sc.stop()
  }
}
