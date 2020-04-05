package com.yyw.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class SparkStreamingWindowOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WindowOperator");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 设置日志级别为WAR
         */
        jsc.sparkContext().setLogLevel("WARN");

        /**
         * 注意：
         * 1、没有优化的窗口函数可以不设置checkpoint目录
         * 2、优化的窗口函数必须设置checkpoint目录
         */
        //jsc.checkpoint("hdfs://node01:9000/spark/checkpoint");
        jsc.checkpoint("./checkpoint");

        JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("node05", 9999);

        JavaDStream<String> linesDStream = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairWords = linesDStream.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        /**
         * 非优化window窗口操作
         * 假设每隔10秒，计算最近60秒内的数据，那么这个窗口大小就是60秒，里面有12个rdd, 在没有计算前，这些rdd是不会进行计算的。
         * 在计算的时候会将这12个rdd聚合起来，然后一起执行reduceByKeyAndWindow操作
         * reduceByKeyAndWindow是针对窗口操作的而不是针对DStream操作的
         */
        //JavaPairDStream<String, Integer> reduceByKeyAndWindowWords = pairWords.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
        //    @Override
        //    public Integer call(Integer v1, Integer v2) throws Exception {
        //        return v1 + v2;
        //    }
        //}, Durations.seconds(15), Durations.seconds(5));


        /**
         * window窗口操作优化: 加上新入的批次，减去出去的批次
         */
        JavaPairDStream<String, Integer> reduceByKeyAndWindowWords = pairWords.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1-v2;
            }
        }, Durations.seconds(15), Durations.seconds(5));


        reduceByKeyAndWindowWords.print();
        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.close();
    }


}
