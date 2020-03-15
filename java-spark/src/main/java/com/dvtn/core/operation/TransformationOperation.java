package com.dvtn.core.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class TransformationOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("TransformationOperation");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("./java-spark/data/words");



        /**
         * filter(f:T=>Bool): RDD[T] => RDD[T]
         *      过滤RDD时内容，不改变RDD的类型
         */

        //JavaRDD<String> filter = lines.filter(new Function<String, Boolean>() {
        //    @Override
        //    public Boolean call(String s) throws Exception {
        //        return s.equalsIgnoreCase("hello scala");
        //    }
        //});
        //filter.foreach(new VoidFunction<String>() {
        //    @Override
        //    public void call(String s) throws Exception {
        //        System.out.println(s);
        //    }
        //});


        /**
         * sample(boolean withReplacement, double fraction, long seed)
         * 参数
         *      withReplacement: 有无放回抽样
         *      fraction: 抽样比例
         *      seed: 抽样种子
         *
         */
        //JavaRDD<String> sample = lines.sample(true, 0.1, 100);
        //sample.foreach(new VoidFunction<String>() {
        //    @Override
        //    public void call(String s) throws Exception {
        //        System.out.println(s);
        //    }
        //});


        sc.stop();
    }
}
