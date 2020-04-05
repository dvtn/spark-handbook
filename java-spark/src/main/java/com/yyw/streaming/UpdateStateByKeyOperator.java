package com.yyw.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * UpdateStateByKey的主要功能：
 * 1、为Spark Streaming中每一个Key维护一份state状态，state类型可以是任意类型，可以是一个自定义对象，那么更新函数是可以自定义的。
 * 2、通过更新函数对该Key的状态不断更新，对于每个新的batch而言，SparkStreaming会在使用updateStateByKey的时候为已经存在的key进行
 *
 *
 * 如果要不断的更亲每个key的state,就一宁涉及到了状态的保存和容错，这个时候需要开启checkpoint机制功能
 *
 * 应用：
 *  全面的广告点击分析
 *  统计一天车流量
 *  统计点击量
 */
public class UpdateStateByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("updateStateByKeyOperator");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        /**
         * 设置checkpoint目录
         *
         * 多久会将内存中的数据（每一个key所对应的状态)写入一磁盘上一份呢？
         *  如果batchInterval小于10s, 那么10s会将内存中的数据写入到磁盘一份
         *  如果batchInterval大小10s, 那么以batchInterval为准
         *  这样做是为了防止频繁地写HDFS
         */
        //JavaSparkContext sc = jsc.sparkContext();
        //sc.setCheckpointDir("./checkpoint");
        //jsc.checkpoint("hdfs://node01:9000/spark/checkpoint");
        jsc.checkpoint("./checkpoint");

        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("node05",9999);

        JavaDStream<String> words = lines.flatMap((line) -> {
            return Arrays.asList(line.split(" ")).iterator();
        });

        JavaPairDStream<String, Integer> pairWords = words.mapToPair((word) -> {
            return new Tuple2<String, Integer>(word, 1);
        });

        JavaPairDStream<String, Integer> updateStateByKeyWords = pairWords.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                /**
                 * values: 经过分组后这个Key所对应的value [1,2,1,3,1]
                 * state: 这个Key在本次之前的状态
                 */
                Integer updateValue = 0;
                if(state.isPresent()){
                    updateValue = state.get();
                }
                for(Integer value: values){
                    updateValue += value;
                }
                return Optional.of(updateValue);
            }
        });

        //OutputOperator
        updateStateByKeyWords.print();

        jsc.start();
        try {
            jsc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        jsc.close();

    }
}
