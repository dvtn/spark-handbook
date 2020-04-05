package com.yyw.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * 并行度：
 * 1、linesDStream里面封装得到的是RDD, RDD里面有partition与读取的topic的partition数是一致的；
 * 2、从kafka中读出来的数据封装到一个DStream里面，可以以这个DStream重新分区repartitions(numPartitions)
 */
public class SparkStreamingOnKafkaDirect {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkStreamingOnKafkaDirect");
        //conf.set("spark.streaming.backpressure.enabled","false");
        //conf.set("spark.streaming.kafka.maxRatePerPartition","100");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        /**
         * 可以不设置checkpoint, 不设置不保存offset, offset默认在内存中有一份，
         * 如果设置checkpoint,则在checkpoint中也有一份offset,一般要设置
         */
        jsc.checkpoint("./checkpoint");

        String brokers = "node02:9092,node03:9092,node04:9092";
        String groupId = "kafka-01";
        String topics = "t0404";
        Set<String> topicSet = new HashSet<>(Arrays.asList(topics.split(",")));


        Map<String,Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
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

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
