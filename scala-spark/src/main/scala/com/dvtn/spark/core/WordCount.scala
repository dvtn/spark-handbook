package com.dvtn.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("./scala-spark/data/words").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).foreach(println)

    sc.stop()


//    val conf = new SparkConf()
//    conf.setMaster("local").setAppName("wordcount")
//
//    val sc = new SparkContext(conf)
//
//    val lines: RDD[String] = sc.textFile("./scala-spark/data/words")
//
//    val words: RDD[String] = lines.flatMap(line => {
//      line.split(" ")
//    })
//
//    val pairWords: RDD[(String, Int)] = words.map(word => {
//      new Tuple2(word, 1)
//    })
//
//    val reducedWords: RDD[(String, Int)] = pairWords.reduceByKey((v1: Int, v2: Int) => {
//      v1 + v2
//    })
//
//    reducedWords.foreach(tuple => {
//      println(tuple)
//    })
//
//    sc.stop()

  }
}
