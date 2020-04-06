package com.yyw.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * transform操作
 *  DStream可以通过transform做RDD到RDD的任意操作。
 */
public class SparkStreamingTransformOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("SparkStreamingTransformOperator");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //黑名单
        List<String> list = Arrays.asList("zhangsan");
        final Broadcast<List<String>> bcBlackList = jsc.sparkContext().broadcast(list);

        //接收socket数据
        JavaReceiverInputDStream<String> nameList = jsc.socketTextStream("node05",9999);

        JavaPairDStream<String, String> pairNameList = nameList.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[1], s);
            }
        });

        /**
         * transform可以拿到DStream中的RDD, 做RDD到RDD的转换，不需要Action算子触发，需要返回RDD类型。
         * 注意：transform call方法内，全到的RDD算子外的代码是在Driver端执行，可以做到动态广播变量
         *
         */
        JavaDStream<String> transformResult = pairNameList.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> pairNamedRDD) throws Exception {
                JavaPairRDD<String, String> filterRDD = pairNamedRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        return !bcBlackList.value().contains(tuple._1);
                    }
                });

                /**
                 * 对RDD使用map算子
                 */
                JavaRDD<String> mapNameRDD = filterRDD.map(new Function<Tuple2<String, String>, String>() {

                    @Override
                    public String call(Tuple2<String, String> tuple) throws Exception {
                        return tuple._2;
                    }
                });

                //返回过滤好的结果
                return mapNameRDD;
            }
        });


        transformResult.print();

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.close();
    }
}
