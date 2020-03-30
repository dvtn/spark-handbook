package com.yyw.sql.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import static  org.apache.spark.sql.functions.col;

public class CreateDataframeFromJson {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("createDataframeFromJson")
                .master("local")
                .getOrCreate();
        Dataset<Row> df = spark.read().json("./java-spark/data/person");

        df.show(100);

        df.printSchema();

        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();

        df.filter(col("age").gt(30)).show();

        df.groupBy("category").count().show();


        df.createOrReplaceTempView("person");
        Dataset<Row> sqlDF = spark.sql("select name, gender, age, category from person");
        sqlDF.show();


        spark.close();
    }
}
