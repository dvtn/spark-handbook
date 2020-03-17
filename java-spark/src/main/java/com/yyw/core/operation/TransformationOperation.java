package com.yyw.core.operation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOperation {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("TransformationOperation");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        ///**
        // * zipWithIndex
        // * 将RDD中的元素和这个元素在RDD中的索引号(从0开始）组成成(K,V)对.
        // */
        //
        //JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("独孤求败", "剑魔"),
        //        new Tuple2<String, String>("独孤求败", "剑魔剑魔"),
        //        new Tuple2<String, String>("风清扬", "独孤九剑"),
        //        new Tuple2<String, String>("风清扬", "独孤九剑"),
        //        new Tuple2<String, String>("张三丰", "太极拳"),
        //        new Tuple2<String, String>("张三丰", "太极拳太极拳")
        //        ));
        //JavaPairRDD<Tuple2<String, String>, Long> zipWithIndexRdd = rdd1.zipWithIndex();
        //zipWithIndexRdd.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Long>>() {
        //    @Override
        //    public void call(Tuple2<Tuple2<String, String>, Long> tuple) throws Exception {
        //        System.out.println(tuple);
        //
        //    }
        //});

        ///**
        // * zip 将两个RDD中的元素(KV格式/非KV格式)变成一个KV格式的RDD, 两个RDD的每个分区元素个数必须相同。
        // *  一对一压缩
        // */
        //
        //JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("独孤求败", "剑魔"),
        //        new Tuple2<String, String>("独孤求败", "剑魔剑魔"),
        //        new Tuple2<String, String>("风清扬", "独孤九剑"),
        //        new Tuple2<String, String>("风清扬", "独孤九剑"),
        //        new Tuple2<String, String>("张三丰", "太极拳"),
        //        new Tuple2<String, String>("张三丰", "太极拳太极拳")
        //));
        //JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, Integer>("独孤求败", 100),
        //        new Tuple2<String, Integer>("独孤求败", 10000),
        //        new Tuple2<String, Integer>("风清扬", 200),
        //        new Tuple2<String, Integer>("风清扬", 20000),
        //        new Tuple2<String, Integer>("张三丰", 300),
        //        new Tuple2<String, Integer>("张三丰", 30000)
        //));
        //
        //JavaPairRDD<Tuple2<String, String>, Tuple2<String, Integer>> zipRdd = rdd1.zip(rdd2);
        //zipRdd.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, Integer>>>() {
        //    @Override
        //    public void call(Tuple2<Tuple2<String, String>, Tuple2<String, Integer>> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});

        ///**
        // * groupByKey
        // *  作用在KV格式的RDD上。根据key进行分组。作用在(K,V)返回(K, Iterable<V>)
        // */
        //JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("独孤求败", "剑魔"),
        //        new Tuple2<String, String>("独孤求败", "剑魔剑魔"),
        //        new Tuple2<String, String>("风清扬", "独孤九剑"),
        //        new Tuple2<String, String>("风清扬", "独孤九剑"),
        //        new Tuple2<String, String>("张三丰", "太极拳"),
        //        new Tuple2<String, String>("张三丰", "太极拳太极拳")
        //));
        //
        //JavaPairRDD<String, Iterable<String>> groupByKeyRdd = rdd1.groupByKey();
        //groupByKeyRdd.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
        //    @Override
        //    public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});


        ///**
        // * mapPartitionWithIndex
        // *  类似于mapPartitions, 除此之外还会携带分区的索引值
        // */
        //JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
        //        "独孤求败", "风清扬", "任我行", "东方不败",
        //        "张学友", "刘德华", "黎明", "郭富城",
        //        "华为", "阿里", "拼多多", "百度"
        //), 3);
        //
        //System.out.println("rdd1 partition size = "+ rdd1.partitions().size());
        //
        //JavaRDD<String> mapPartitionsWithIndex = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
        //    @Override
        //    public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
        //        List<String> list = new ArrayList<>();
        //        while (iterator.hasNext()) {
        //            String value = iterator.next();
        //            //System.out.println("partition index = 【" + index + "】, value = 【" + value + "】");
        //            list.add(" rdd1 partition index = 【" + index + "】, value = 【" + value + "】");
        //        }
        //        return list.iterator();
        //    }
        //}, true);

        //List<String> collect = mapPartitionsWithIndex.collect();
        //for(String s:collect){
        //    System.out.println(s);
        //}


        ///**
        // * repartition
        // *  增加或减少分区。会产生shuffle类的算子。（多个分区分到一个分区不会产生shuffle)
        // *
        // */
        //JavaRDD<String> rdd2 = mapPartitionsWithIndex.repartition(4); //也可以改成2查看减少分区的情况
        //JavaRDD<String> rdd3 = rdd2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
        //    @Override
        //    public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
        //        List<String> list = new ArrayList<>();
        //        while (iterator.hasNext()) {
        //            String value = iterator.next();
        //            //System.out.println("partition index = 【" + index + "】, value = 【" + value + "】");
        //            list.add("rdd2 partition index = 【" + index + "】, value = 【" + value + "】");
        //        }
        //        return list.iterator();
        //    }
        //}, true);
        //
        //
        //List<String> collect = rdd3.collect();
        //for(String s: collect){
        //    System.out.println(s);
        //}




        ///**
        // * coalesce
        // *  coalesce常用来减少分区，第二个参数是减少分区的过程中是否产生shuffle
        // *  true为产生shuffle, false为不产生shuffle。默认是false
        // *  如果coalesce设置的分区数比原来的RDD的分区数还多的话，每二个参数为false时不起作用，如果设置成true, 效果和repartition一样。
        // *  即repartition(numPartitions) = coalesce(numPartitions, true)
        // */
        //JavaRDD<String> coalesceRdd = mapPartitionsWithIndex.coalesce(2,true);
        ////JavaRDD<String> coalesceRdd = mapPartitionsWithIndex.coalesce(4,false);
        ////System.out.println("coalesceRdd partition size: "+ coalesceRdd.partitions().size()); //3
        //JavaRDD<String> rdd4 = coalesceRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
        //    @Override
        //    public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
        //        List<String> list = new ArrayList<>();
        //        while (iterator.hasNext()) {
        //            String value = iterator.next();
        //            //System.out.println("partition index = 【" + index + "】, value = 【" + value + "】");
        //            list.add("coalesceRdd partition index = 【" + index + "】, value = 【" + value + "】");
        //        }
        //        return list.iterator();
        //    }
        //}, true);
        //
        //
        //List<String> collect = rdd4.collect();
        //    for(String s: collect){
        //        System.out.println(s);
        //    }


        ///**
        // * distinct
        // */
        //JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "a", "b", "b", "b", "c", "d"), 3);
        //
        //rdd.distinct().foreach(new VoidFunction<String>() {
        //    @Override
        //    public void call(String s) throws Exception {
        //        System.out.println(s);
        //    }
        //});

        //rdd.mapToPair(new PairFunction<String, String, Integer>() {
        //    @Override
        //    public Tuple2<String, Integer> call(String s) throws Exception {
        //        return new Tuple2<String, Integer>(s,1);
        //    }
        //}).reduceByKey(new Function2<Integer, Integer, Integer>() {
        //    @Override
        //    public Integer call(Integer v1, Integer v2) throws Exception {
        //        return v1+v2;
        //    }
        //}).map(new Function<Tuple2<String, Integer>, String>() {
        //    @Override
        //    public String call(Tuple2<String, Integer> tuple) throws Exception {
        //        return tuple._1;
        //    }
        //}).foreach(new VoidFunction<String>() {
        //    @Override
        //    public void call(String s) throws Exception {
        //        System.out.println(s);
        //    }
        //});


        ///**
        // * mapPartition
        // *  一个分区一个分区地处理数据
        // */
        //
        //JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), 3);
        //rdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
        //    @Override
        //    public Iterator<String> call(Iterator<String> iterator) throws Exception {
        //        List<String> list = new ArrayList<>();
        //        System.out.println("创建数据库连接......");
        //        while(iterator.hasNext()){
        //            String s = iterator.next();
        //            System.out.println("插入数据：...."+s);
        //            list.add(s);
        //        }
        //        System.out.println("关闭数据库连接......");
        //        return list.iterator();
        //    }
        //}).collect();

        //JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), 3);
        //rdd.map(new Function<String, Object>() {
        //    @Override
        //    public Object call(String s) throws Exception {
        //        System.out.println("创建数据库连接...");
        //        System.out.println("插入数据........"+ s);
        //        System.out.println("关闭数据连接........");
        //
        //        return s+"~";
        //
        //    }
        //}).collect();



        ///**
        // * cogroup
        // *  将两个rdd的key全并，每个RDD中的key对应一个value集合
        // */
        //JavaPairRDD<String, String> rdd3 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "武女"),
        //        new Tuple2<String, String>("林青霞", "武女武女"),
        //        new Tuple2<String, String>("林青霞", "武女武女武女"),
        //        new Tuple2<String, String>("风清扬", "剑宗"),
        //        new Tuple2<String, String>("风清扬", "剑宗剑宗"),
        //        new Tuple2<String, String>("独孤求败", "武男")
        //), 3);
        //JavaPairRDD<String, String> rdd4 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "侠女侠女"),
        //        new Tuple2<String, String>("风清扬", "华山"),
        //        new Tuple2<String, String>("风清扬", "华山华山"),
        //        new Tuple2<String, String>("风清扬", "华山华山华山"),
        //        new Tuple2<String, String>("独孤求败", "大侠"),
        //        new Tuple2<String, String>("独孤求败", "大侠大侠"),
        //        new Tuple2<String, String>("东方不败", "非女非男")
        //), 2);
        //
        //JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroupRdd = rdd3.cogroup(rdd4);
        //cogroupRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});



        ///**
        // * subtractRDD求两个rdd的差集
        // */
        //JavaPairRDD<String, String> rdd3 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "侠女"),
        //        new Tuple2<String, String>("独孤求败", "武男"),
        //        new Tuple2<String, String>("风清扬", "55"),
        //        new Tuple2<String, String>("令狐冲", "27"),
        //        new Tuple2<String, String>("王重阳", "40")
        //), 3);
        //JavaPairRDD<String, String> rdd4 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "侠女"),
        //        new Tuple2<String, String>("独孤求败", "武男"),
        //        new Tuple2<String, String>("风清扬", "剑圣"),
        //        new Tuple2<String, String>("郭靖", "响龙十八掌"),
        //        new Tuple2<String, String>("令狐冲", "独孤九剑"),
        //        new Tuple2<String, String>("东方不败", "非女非男")
        //), 2);
        //
        //JavaPairRDD<String, String> subtractRDD = rdd3.subtract(rdd4);
        //System.out.println(" subtractRDD partition size:"+ subtractRDD.partitions().size());
        //subtractRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
        //    @Override
        //    public void call(Tuple2<String, String> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});

        ///**
        // * intersection求两个RDD的交集
        // */
        //
        //JavaPairRDD<String, String> intersectionRdd = rdd3.intersection(rdd4);
        //System.out.println(" intersectionRdd partition size:"+ intersectionRdd.partitions().size());
        //intersectionRdd.foreach(new VoidFunction<Tuple2<String, String>>() {
        //    @Override
        //    public void call(Tuple2<String, String> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});

        ///**
        // * union合并RDD,类型要求一致
        // * 合并后的分区数是两个rdd的分区之和
        // */
        //JavaPairRDD<String, String> rdd3 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "30"),
        //        new Tuple2<String, String>("独孤求败", "70"),
        //        new Tuple2<String, String>("风清扬", "55"),
        //        new Tuple2<String, String>("令狐冲", "27"),
        //        new Tuple2<String, String>("王重阳", "40")
        //), 3);
        //JavaPairRDD<String, String> rdd4 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "侠女"),
        //        new Tuple2<String, String>("独孤求败", "武男"),
        //        new Tuple2<String, String>("风清扬", "剑圣"),
        //        new Tuple2<String, String>("郭靖", "响龙十八掌"),
        //        new Tuple2<String, String>("令狐冲", "独孤九剑"),
        //        new Tuple2<String, String>("东方不败", "非女非男")
        //), 2);
        //
        //JavaPairRDD<String, String> unionRdd = rdd3.union(rdd4);
        //System.out.println("RDD partition size:"+ unionRdd.partitions().size());
        //unionRdd.foreach(new VoidFunction<Tuple2<String, String>>() {
        //    @Override
        //    public void call(Tuple2<String, String> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});


        //JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, String>("林青霞", "30"),
        //        new Tuple2<String, String>("独孤求败", "70"),
        //        new Tuple2<String, String>("风清扬", "55"),
        //        new Tuple2<String, String>("令狐冲", "27"),
        //        new Tuple2<String, String>("王重阳", "40")
        //), 3);
        //JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(Arrays.asList(
        //        new Tuple2<String, Integer>("林青霞", 100),
        //        new Tuple2<String, Integer>("独孤求败", 200),
        //        new Tuple2<String, Integer>("风清扬", 300),
        //        new Tuple2<String, Integer>("郭靖", 400),
        //        new Tuple2<String, Integer>("令狐冲", 500),
        //        new Tuple2<String, Integer>("东方不败", 600)
        //), 2);


        ///**
        // * fullOuterJoin
        // */
        //JavaPairRDD<String, Tuple2<Optional<String>, Optional<Integer>>> fullOuterJoinRdd = rdd1.fullOuterJoin(rdd2);
        //System.out.println("RDD partition size:"+ fullOuterJoinRdd.partitions().size());
        //fullOuterJoinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<String>, Optional<Integer>>>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2<Optional<String>, Optional<Integer>>> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});

        ///**
        // * rightOuterJoin
        // */
        //JavaPairRDD<String, Tuple2<Optional<String>, Integer>> rightOuterJoinRdd = rdd1.rightOuterJoin(rdd2);
        //System.out.println("RDD partition size:"+ rightOuterJoinRdd.partitions().size());
        //rightOuterJoinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<String>, Integer>>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2<Optional<String>, Integer>> tuple) throws Exception {
        //        String key = tuple._1;
        //        Optional<String> value1 = tuple._2._1;
        //        Integer value2 = tuple._2._2;
        //
        //        if(value1.isPresent()){
        //            System.out.println("key= "+key+", value1 = "+value1.get()+", value2 = "+ value2);
        //        } else {
        //            System.out.println("key= "+key+", value1 = Null, value2 = "+ value2);
        //        }
        //
        //    }
        //});

        ///**
        // * leftOuterJoin
        // *
        // */
        //JavaPairRDD<String, Tuple2<String, Optional<Integer>>> leftOuterJoinRdd = rdd1.leftOuterJoin(rdd2);
        //System.out.println("RDD partition size:"+ leftOuterJoinRdd.partitions().size());
        //leftOuterJoinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Optional<Integer>>>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2<String, Optional<Integer>>> tuple) throws Exception {
        //        String key = tuple._1;
        //        String value1 = tuple._2._1;
        //        Optional<Integer> optionV = tuple._2._2;
        //        if(optionV.isPresent()) {
        //            System.out.println("key: " + key + ", value1 =" + value1 + ", value2 =" + optionV.get());
        //        }else {
        //            System.out.println("key: " + key + ", value1 =" + value1 + ", value2 = Null" );
        //        }
        //        //System.out.println(tuple);
        //    }
        //});

        ///**
        // * leftOuterJoin
        // *
        // */
        //JavaPairRDD<String, Tuple2<String, Optional<Integer>>> leftOuterJoinRdd = rdd1.leftOuterJoin(rdd2);
        //System.out.println("RDD partition size:"+ leftOuterJoinRdd.partitions().size());
        //leftOuterJoinRdd.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Optional<Integer>>>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2<String, Optional<Integer>>> tuple) throws Exception {
        //        String key = tuple._1;
        //        String value1 = tuple._2._1;
        //        Optional<Integer> optionV = tuple._2._2;
        //        if(optionV.isPresent()) {
        //            System.out.println("key: " + key + ", value1 =" + value1 + ", value2 =" + optionV.get());
        //        }else {
        //            System.out.println("key: " + key + ", value1 =" + value1 + ", value2 = Null" );
        //        }
        //        //System.out.println(tuple);
        //    }
        //});

        ///**
        // * join
        // * 按照两个RDD的key关联
        // * 作用在K,V格式的RDD上。根据K进行连接，对(K,V) join(K,W) 返回(K,(V,W)
        // * join后的分区数与父RDD分区数多的那一个相同
        // */
        //JavaPairRDD<String, Tuple2<String, Integer>> joinRDD = rdd1.join(rdd2);
        //System.out.println("RDD partition size:"+ joinRDD.partitions().size());
        //joinRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Integer>>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2<String, Integer>> tuple) throws Exception {
        //        System.out.println(tuple);
        //    }
        //});


        ///**
        // * sc.parallelize(list:List[T],numSlices:Int)
        // * numSlices:批定分区数；不指定该参数的默认的分区数是1.
        // */
        //JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e", "f"),3);
        //System.out.println("parallelize创建rdd的默认分区数个数："+rdd.partitions().size());
        //List<String> collect = rdd.collect();
        


        //JavaRDD<String> lines = sc.textFile("./java-spark/data/words");
        //
        //
        //
        ///**
        // * filter(f:T=>Bool): RDD[T] => RDD[T]
        // *      过滤RDD时内容，不改变RDD的类型
        // */

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


        ///**
        // * sample(boolean withReplacement, double fraction, long seed)
        // * 参数
        // *      withReplacement: 有无放回抽样
        // *      fraction: 抽样比例
        // *      seed: 抽样种子
        // *
        // */
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
