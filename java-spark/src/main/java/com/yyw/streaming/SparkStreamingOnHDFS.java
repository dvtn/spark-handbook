package com.yyw.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * Spark standalone or Mesos with cluster deploy mode only:
 * 在提交application的时候，添加 --supervisor 选项，如果Driver挂掉会自动启动一个Driver
 *
 */

public class SparkStreamingOnHDFS {
    public static void main(String[] args) throws InterruptedException {
        final SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkStreamingOnHDFS");

        //final String checkPointDirectory = "hdfs://node01:9000/spark/SparkStreaming/CheckPoint2020";
        final String checkPointDirectory = "./checkpoint";

        JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(checkPointDirectory, new Function0<JavaStreamingContext>() {
            @Override
            public JavaStreamingContext call() throws Exception {
                JavaStreamingContext jsc = new JavaStreamingContext(conf, Seconds.apply(5));
                jsc.checkpoint(checkPointDirectory);


                String brokers = "node02:9092,node03:9092,node04:9092";
                String groupId = "kafka-01";
                String topics = "t0404";
                Set<String> topicSet = new HashSet<>(Arrays.asList(topics.split(",")));


                Map<String,Object> kafkaParams = new HashMap<>();
                kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
                kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
                kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
                kafkaParams.put("auto.offset.reset","latest");
                kafkaParams.put("enable.auto.commit",false);

                JavaInputDStream<ConsumerRecord<String,String>> messages = KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String,String>Subscribe(topicSet,kafkaParams)
                );

                JavaDStream<String> lines = messages.map(new Function<ConsumerRecord<String, String>, String>() {
                    Map<String, String> map = new HashMap<>();
                    @Override
                    public String call(ConsumerRecord<String, String> record) throws Exception {
                        return  record.value();
                    }
                });

                JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return Arrays.asList(s.split("\t")).iterator();
                    }
                });

                JavaPairDStream<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word,1);
                    }
                });

                JavaPairDStream<String, Integer> wordsCount = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                });

                wordsCount.print(100);

                return jsc;
            }
        });


        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
