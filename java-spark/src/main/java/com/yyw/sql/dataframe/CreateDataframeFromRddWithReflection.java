package com.yyw.sql.dataframe;

import com.yyw.pojo.People;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class CreateDataframeFromRddWithReflection {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateDataframeFromRddWithReflectionApp")
                .getOrCreate();
        //从文本对象创建Person类型的RDD
        JavaRDD<People> peopleRDD = spark.read()
                .textFile("./java-spark/data/People")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    People people = new People();
                    people.setId(Integer.parseInt(parts[0]));
                    people.setName(parts[1]);
                    people.setAge(Integer.parseInt(parts[2]));
                    people.setCategory(parts[3]);
                    people.setPosition(parts[4]);
                    return people;
                });
        //根据反射把JavaBeans作为schema创建DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, People.class);
        //把DataFrame注册为SparkSession内的临时视图
        peopleDF.createOrReplaceTempView("person");
        //通过sql的方式查询
        Dataset<Row> shuDF = spark.sql("select id, name, age, category, position from person where category='蜀'");
        //查看结果
        shuDF.show();

        //通过列索引查看数据
        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> shuNamesByIndexDF = shuDF.map(
                (MapFunction<Row, String>) row -> "Name:" + row.getString(1), stringEncoder
        );
        shuNamesByIndexDF.show();

        //通过列名
        Dataset<String> shuNameByFieldDF = shuDF.map(
                (MapFunction<Row, String>) row -> "Name:" + row.getAs("name"), stringEncoder
        );
        shuNameByFieldDF.show();



        spark.close();

    }
}
