package com.dvtn.spark.core.operation

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationOperationTest")
    val sc = new SparkContext(conf)

    /**
     * minPartitions:
     *  指定生成的RDD的分区数,增加该参数可增加task的并行度
     */
    val lines = sc.textFile("./scala-spark/data/words", 3)


    /**
     * sample(withReplacement = , fraction = , seed = ) 抽样
     * 参数说明：
     *  withReplacement: Boolean,表示是否是放回抽样
     *  fraction: Double, 抽样比例
     *  seed: Long,种子,针对同一批数据只要定义的种子相同表示每次抽样的结果是否相同
     */
//    val result = lines.sample(true , 0.1 , 100)
//    result.foreach(println)

    /**
     * filter
     */
    lines.filter(s=>{
      s.endsWith("hello world")
    }).foreach(println)


    sc.stop()


  }

}
