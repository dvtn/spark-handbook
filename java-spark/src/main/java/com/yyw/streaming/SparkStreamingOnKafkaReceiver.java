package com.yyw.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SparkStreamingOnKafkaReceiver {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("SparkStreamingOnKafkaReceiver")
                .setMaster("local[2]");
        //开启WAL预写日志机制
        conf.set("spark.streaming.receiver.writeAheadLog.enable","true");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("./receivedata");

        Map<String, Integer> topicConsumerConcurrency = new HashMap<>();
        /**
         * 设置读取的topic接收数据的线程数
         */
        topicConsumerConcurrency.put("t0404",1);
        /**
         * 第一个参数是StreamingContext
         * 每二个参数是Zookeeper的集群信息（接收Kafka数据时会从Zookeeper中获取Offset等元数据信息）
         * 第三个参数是Consumer Group消费者组
         * 每四个参数是消费的Topic以及并发读取Topic中的Partition的线程数
         *
         * 注意：
         * KafkaUtils.createStream使用五个参数的方法，设置Receiver模式的存储级别
         */
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                 jsc,
                "node02:2181,node03:2181,node04:2181",
                "MyFirstConsumerGroup",
                topicConsumerConcurrency
        );

        //JavaPairReceiverInputDStream<String,String> lines = KafkaUtils.createStream(
        //        jsc,
        //        "node03:2181,node04:2181,node05:2181",
        //        "MyFirstConsumerGroup",
        //        topicConsumerConcurrency,
        //        StorageLevel.MEMORY_AND_DISK() //默认的存储级别是StorageLevel.MEMORY_AND_DISK_SER_2
        //);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2.split("\t")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKeyWords = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        reduceByKeyWords.print(100);

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
