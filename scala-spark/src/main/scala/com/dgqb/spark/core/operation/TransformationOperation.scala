package com.dgqb.spark.core.operation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object TransformationOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationOperationTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /**
     * groupByKey
     */
    val rdd = sc.parallelize(Array(
      ("金庸","飞狐外传"),
      ("金庸","雪山飞弧"),
      ("金庸","连成诀"),
      ("金庸","天龙八部"),
      ("金庸","白马啸西风"),
      ("金庸","鹿鼎记"),
      ("金庸","笑傲江湖"),
      ("金庸","书剑恩仇录"),
      ("金庸","神雕侠女"),
      ("金庸","侠客行"),
      ("金庸","倚天屠龙记"),
      ("金庸","碧血剑"),
      ("金庸","鸳鸯刀"),
      ("古龙","小李飞刀"),
      ("古龙","楚留香传奇"),
      ("古龙","陆小凤传奇"),
      ("古龙","浣花洗剑录"),
      ("古龙","武林外史"),
      ("古龙","萧十一郎"),
      ("古龙","绝代双骄"),
      ("古龙","流星·蝴蝶·剑")

    ), 3);

    rdd.groupByKey().foreach(println)


//    /**
//     * zipWithIndex
//     */
//    val rdd1 = sc.parallelize(Array(
//      "飞狐外传","雪山飞弧","连成诀","天龙八部",
//      "射雕英雄传","白马啸西风","鹿鼎记","笑傲江湖",
//      "书剑恩仇录","神雕侠女","侠客行","倚天屠龙记"
//    ),3);
//
//    rdd1.zipWithIndex().foreach(println)

//    /**
//     * zip
//     * 把两个rdd拼接成KV格式的rdd
//     */
//    val rdd1 = sc.parallelize(Array(
//      "飞狐外传","雪山飞弧","连成诀","天龙八部",
//      "射雕英雄传","白马啸西风","鹿鼎记","笑傲江湖",
//      "书剑恩仇录","神雕侠女","侠客行","倚天屠龙记"
//    ),3);
//    val rdd2 = sc.parallelize(Array(
//      "胡斐","苗人凤","狄云","段誉",
//      "郭靖","上官虹","韦小宝","令狐冲",
//      "陈家洛","杨过","石中玉","张君宝"
//    ),3);
//    rdd1.zip(rdd2).foreach(println)


//
//    /**
//     * mapPartitionWithIndex
//     */
//    val rdd = sc.parallelize(Array(
//      "飞狐外传","雪山飞弧","连成诀","天龙八部",
//      "射雕英雄传","白马啸西风","鹿鼎记","笑傲江湖",
//      "书剑恩仇录","神雕侠女","侠客行","倚天屠龙记"
//    ),3);
//    val rdd1 = rdd.mapPartitionsWithIndex((index, iter)=>{
//      val list = new ListBuffer[String]()
//      while(iter.hasNext){
//        list.append("rdd partition index = "+index+", value = "+ iter.next())
//      }
//      list.iterator
//    },true)
//
//    rdd1.foreach(println)
//
//    /**
//     * repartition
//     */
//    rdd1.repartition(4).mapPartitionsWithIndex((index,iter)=>{
//      val list = new ListBuffer[String]()
//      while(iter.hasNext){
//        list.append("repartition partition index = "+index+", value = "+ iter.next())
//      }
//      list.iterator
//    },true).foreach(println)
//
//    /**
//     * coalesce
//     */
//    rdd1.coalesce(4,true).mapPartitionsWithIndex((index, iter)=>{
//      val list = new ListBuffer[String]()
//      while(iter.hasNext){
//        list.append("coalesce partition index = "+index+", value = "+ iter.next())
//      }
//      list.iterator
//    },true).foreach(println)



    //    /**
//     * mapPartitions 一个分区一个分区地处理数据
//     */
//    val rdd = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",40),("张三丰",80),("独孤求败",60),("风清扬",40),("张三丰",80)),3)
//    rdd.mapPartitions(iter => {
//      println("插入数据库...........")
//      iter
//    },true).collect()

//    /**
//     * distinct:去重
//     */
//    val rdd = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",40),("张三丰",80),("独孤求败",60),("风清扬",40),("张三丰",80)),3)
//    println("----------->distinct<-------------")
//    rdd.distinct().foreach(println)


//    val rdd1 = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",40),("张三丰",80)),3)
//    val rdd2 = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",3000),("王重阳",4000)),3)
//
//    /**
//     * subtract:取关集
//     */
//    println("----------->intersection<-------------")
//    rdd1.subtract(rdd2).foreach(println)
//
//
//    /**
//     * intersection:取交集
//     */
//    println("----------->intersection<-------------")
//    rdd1.intersection(rdd2).foreach(println)



    //    val rdd1 = sc.makeRDD(Array(("林青霞",28),("独孤求败",60),("风清扬",40),("张三丰",80)),3)
//    val rdd2 = sc.makeRDD(Array(("林青霞",1000),("独孤求败",2000),("风清扬",3000),("王重阳",4000)),3)
//
//    /**
//     * union
//     */
//    println("----------->union<-------------")
//    rdd1.union(rdd2).foreach(println)
//
//
//    /**
//     * join: 对(K,V) join (K,W) 返回(K,(V,W)
//     *
//     * join后的分区数与父RDD分区数多的那一个相同
//     *
//     *
//     * (风清扬,(40,3000))
//     * (独孤求败,(60,2000))
//     * (林青霞,(28,1000))
//     */
//    println("----------->join<-------------")
//    rdd1.join(rdd2).foreach(println)
//
//    /**
//     * leftOuterJoin
//     */
//    println("----------->leftOuterJoin<-------------")
//    rdd1.leftOuterJoin(rdd2).foreach(println)
//
//    println("----------->rightOuterJoin<-------------")
//    rdd1.rightOuterJoin(rdd2).foreach(println)
//
//    println("----------->fullOuterJoin<-------------")
//    rdd1.fullOuterJoin(rdd2).foreach(println)


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
