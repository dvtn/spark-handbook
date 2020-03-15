package com.dvtn.core.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.List;

public class ActionOperator {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("ActionOperator");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("./java-spark/data/words");

        /**
         * foreach()遍历结果
         */
        lines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        /**
         * collect(): Driver回收结果，当结果太大时可能会出现OOM
         */
        List<String> collect = lines.collect();
        for(String s:collect){
            System.out.println(s);
        }

        /**
         * take(int num):返回结果的前num行
         */
        List<String> take = lines.take(5);
        for(String s:take){
            System.out.println(s);
        }

        /**
         * first(): 返回结果的第一行数据，相当于take(1)
         */
        String first = lines.first();
        System.out.println("first():"+first);


        /**
         * count(): Driver端回收计数结果
         */
        long count = lines.count();
        System.out.println("count():"+count);


        sc.stop();
    }
}
