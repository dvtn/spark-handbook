package com.yyw.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkStreamingWordCount");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(15));

        jsc.sparkContext().setLogLevel("WARN");

        JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("node05", 9999);

        JavaDStream<String> words = socketTextStream.flatMap((lines) -> {
            return Arrays.asList(lines.split(" ")).iterator();
        });

        JavaPairDStream<String, Integer> pairWords = words.mapToPair((word) -> {
            return new Tuple2<String, Integer>(word, 1);
        });

        JavaPairDStream<String, Integer> reduceByKeyWords = pairWords.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });

        //reduceByKeyWords.print(100);
        /**
         * foreachRDD可以拿到DStream中的RDD，对拿到的RDD可以使用RDD的transformation类算子转换
         * 要对拿到的RDD使用action算子触发执行，否则foreachRDD不会执行。
         *
         * foreachRDD中，拿到RDD的算子外的代码是在Driver端执行的。可以使用这个算子实现动态改变广播变量
         */
        reduceByKeyWords.foreachRDD((pairRDD)->{
            /**
             * 这里如果有代码是在Driver端执行
             */
            System.out.println("我是在Driver端执行的....");

            //要在这里定义广播变量，需要使用SparkContext, 而这里的SparkContext只能从rdd拿
            SparkContext context = pairRDD.context();
            JavaSparkContext sc = new JavaSparkContext(context);
            Broadcast<String> broadcast = sc.broadcast("hello");
            String value = broadcast.value();
            System.out.println(value);

            JavaPairRDD<String, Integer> mapToPairRdd = pairRDD.mapToPair((tuple) -> {
                return new Tuple2<String, Integer>(tuple._1.concat("~"), tuple._2);
            });

            mapToPairRdd.foreach((tup)->{
                System.out.println(tup);
            });
        });

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.close();

    }
}
