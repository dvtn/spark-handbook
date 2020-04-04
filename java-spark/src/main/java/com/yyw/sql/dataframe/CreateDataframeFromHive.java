package com.yyw.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class CreateDataframeFromHive {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("spark://node01:7077,node02:7077")
                .appName("createDataframeFromHiveApp")
                .config("hive.metastore.uris", "thrift://node01:9083,node02:9083")
                .config("hive.metastore.warehouse.dir","hdfs://node01:9000/user/hive/warehouse")
                //指定hive的warehouse目录
                .config("spark.sql.warehouse.dir","hdfs://node01:9000/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR"); //设置控制台日志显示的级别
        spark.sql("show databases").show();

        spark.close();

    }
}
