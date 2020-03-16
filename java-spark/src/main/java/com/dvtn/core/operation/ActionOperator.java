package com.dvtn.core.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ActionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("ActionOperator");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), 3);

        /**
         * foreachPartition
         *  一个分区一个分区处理
         */

        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
            @Override
            public void call(Iterator<String> iterator) throws Exception {
                System.out.println("创建数据库连接...");
                while (iterator.hasNext()){
                    String s = iterator.next();
                    System.out.println("拼接sql......"+s);

                }
                System.out.println("关闭数据连接........");
            }
        });

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
