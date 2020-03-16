package com.dgqb.spark.core.operation

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationOperationTest")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",40),("张三丰",80)),3)
    val rdd2 = sc.makeRDD(Array(("林青霞",1000),("独孤求败",2000),("风清扬",3000),("王重阳",4000)),3)
    rdd1.join(rdd2).foreach(println)

    /**
     * join
     */


    /**
     * join, leftOuterJoin, rightOuterJoin, fullOuterJoin
     * 作用在K,V格式的RDD上。根据K进行连接，对(K,V) join(K,W) 返回(K,(V,W)
     * join后的分区数与父RDD分区数多的那一个相同
     */



    //    /**
//     * minPartitions:
//     *  指定生成的RDD的分区数,增加该参数可增加task的并行度
//     */
//    val lines = sc.textFile("./scala-spark/data/words", 3)


//    /**
//     * sample(withReplacement = , fraction = , seed = ) 抽样
//     * 参数说明：
//     *  withReplacement: Boolean,表示是否是放回抽样
//     *  fraction: Double, 抽样比例
//     *  seed: Long,种子,针对同一批数据只要定义的种子相同表示每次抽样的结果是否相同
//     */
//    val result = lines.sample(true , 0.1 , 100)
//    result.foreach(println)

//    /**
//     * filter
//     */
//    lines.filter(s=>{
//      s.endsWith("hello world")
//    }).foreach(println)


    sc.stop()


  }

}
