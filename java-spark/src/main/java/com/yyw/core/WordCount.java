package com.yyw.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCount {
    public static void main(String[] args) {
        /**
         * SparkConf:
         * 1.conf可以设置spark的运行模式
         * 2.可以设置spark在webui中显示的application名称。
         * 3.设置当前Spark Application运行的资源(内存与core)。(通过set方法)
         *      比如电脑是4核8线程：说明每个核是双线程。spark中的cores与线程数相对应。说明这台电脑可以提供spark应用8个cores。
         *
         * spark运行模式
         *  1.local -- 在eclipse,IDEA中开发spark程序要用local模式，本地模式，多用于测试模式。
         *  2.standalone: spark自带的资源调度框架，支持分布式搭建，spark任务可以依赖standalone调度资源。
         *  3.yarn: hadoop生态圈中资源调度框架，spark也可以yarn调度资源。
         *  4.mesos: 资源调度框架。
         */
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("JavaSparkWordCount");
        //conf.set("spark.submit.deployMode","client" );

        /**
         * SparkContext:
         * 它是通往集群的唯一通道。
         */
        JavaSparkContext jsc = new JavaSparkContext(conf);

        /**
         * sc.textFile 读取文件
         */
        JavaRDD<String> lines = jsc.textFile("./java-spark/data/words");

        /**
         * flatMap：进一条数据出多条数据
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        /**
         * 在java中如果想让某个RDD转换成K,V格式使用xxxToPair
         */
        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        /**
         * reduceByKey
         * 1.先将相同的key分组
         * 2.对每一组的key对应的value去按照自定义的逻辑进行处理
         */
        JavaPairRDD<String, Integer> reduceWords = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * 排序
         */
        JavaPairRDD<String, Integer> sortedWords = reduceWords.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                //return new Tuple2<Integer, String>(tuple._2, tuple._1);
                return tuple.swap();
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                //return new Tuple2<String, Integer>(tuple._2, tuple._1);
                return tuple.swap();
            }
        });


        sortedWords.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple);
            }
        });

        jsc.stop();

    }
}
