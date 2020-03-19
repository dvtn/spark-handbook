package com.dgqb.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PvUvApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("PvUvApp")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val lines: RDD[String] = sc.textFile("./scala-spark/data/pvuvdata")
    val websites: RDD[(String, Int)] = lines.map(line => {
      (line.split("\t")(5), 1)
    })
    val websiteReduceByKeyRdd: RDD[(String, Int)] = websites.reduceByKey((v1, v2) => {
      v1 + v2
    })
    websiteReduceByKeyRdd.sortBy(tuple=>tuple._2, false).foreach(println)


    sc.stop();

  }

}
