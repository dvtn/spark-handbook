package com.yyw.core.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ActionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("ActionOperator");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, Integer>("独孤求败", 100),
        //        new Tuple2<String, Integer>("独孤求败", 10000),
        //        new Tuple2<String, Integer>("风清扬", 200),
        //        new Tuple2<String, Integer>("风清扬", 20000),
        //        new Tuple2<String, Integer>("张三丰", 300),
        //        new Tuple2<String, Integer>("张三丰", 30000)
        //));

        ///**
        // * countByValue: 根据数据集每个元素相同的内容来计数。返回相同内容的元素对应的条数(Key和Value要同时相同)
        // */
        //JavaPairRDD<String, Integer> countByValueRdd = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, Integer>("独孤求败", 100),
        //        new Tuple2<String, Integer>("独孤求败", 100),
        //        new Tuple2<String, Integer>("风清扬", 200),
        //        new Tuple2<String, Integer>("风清扬", 20000),
        //        new Tuple2<String, Integer>("张三丰", 300),
        //        new Tuple2<String, Integer>("张三丰", 300)
        //));
        //Map<Tuple2<String, Integer>, Long> countByValue = countByValueRdd.countByValue();
        //Set<Map.Entry<Tuple2<String, Integer>, Long>> entrySet = countByValue.entrySet();
        //for(Map.Entry<Tuple2<String, Integer>, Long> entry:entrySet){
        //    System.out.println("Key = "+ entry.getKey() + ", Value = " + entry.getValue());
        //}

        ///**
        // * countByKey:作用到K,V格式的RDD上，根据Key计算相同Key的数据集元素
        // */
        //JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, Integer>("独孤求败", 100),
        //        new Tuple2<String, Integer>("独孤求败", 100),
        //        new Tuple2<String, Integer>("风清扬", 200),
        //        new Tuple2<String, Integer>("风清扬", 20000),
        //        new Tuple2<String, Integer>("张三丰", 300),
        //        new Tuple2<String, Integer>("张三丰", 300)
        //));
        //Map<String, Long> countByKey = rdd.countByKey();
        //Set<Map.Entry<String, Long>> entrySet = countByKey.entrySet();
        //for(Map.Entry<String,Long> entry:entrySet){
        //    System.out.println("Key = "+ entry.getKey() + ", Value = " + entry.getValue());
        //}

        ///**
        // * reduce: 根据聚合逻辑聚合数据集中的每一个元素
        // */
        //JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        //Integer result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
        //    @Override
        //    public Integer call(Integer v1, Integer v2) throws Exception {
        //        return v1 + v2;
        //    }
        //});
        //System.out.println("reduce result: " + result);


        //JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), 3);
        //
        ///**
        // * foreachPartition
        // *  一个分区一个分区处理
        // */
        //
        //rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
        //    @Override
        //    public void call(Iterator<String> iterator) throws Exception {
        //        System.out.println("创建数据库连接...");
        //        while (iterator.hasNext()){
        //            String s = iterator.next();
        //            System.out.println("拼接sql......"+s);
        //
        //        }
        //        System.out.println("关闭数据连接........");
        //    }
        //});

        //rdd.foreach(new VoidFunction<String>() {
        //    @Override
        //    public void call(String s) throws Exception {
        //                System.out.println("创建数据库连接...");
        //                System.out.println("插入数据........"+ s);
        //                System.out.println("关闭数据连接........");
        //    }
        //});

        //JavaRDD<String> lines = sc.textFile("./java-spark/data/words");
        //
        ///**
        // * foreach()遍历结果
        // */
        //lines.foreach(new VoidFunction<String>() {
        //    @Override
        //    public void call(String s) throws Exception {
        //        System.out.println(s);
        //    }
        //});
        //
        ///**
        // * collect(): Driver回收结果，当结果太大时可能会出现OOM
        // */
        //List<String> collect = lines.collect();
        //for(String s:collect){
        //    System.out.println(s);
        //}
        //
        ///**
        // * take(int num):返回结果的前num行
        // */
        //List<String> take = lines.take(5);
        //for(String s:take){
        //    System.out.println(s);
        //}
        //
        ///**
        // * first(): 返回结果的第一行数据，相当于take(1)
        // */
        //String first = lines.first();
        //System.out.println("first():"+first);
        //
        //
        ///**
        // * count(): Driver端回收计数结果
        // */
        //long count = lines.count();
        //System.out.println("count():"+count);


        sc.stop();
    }
}
