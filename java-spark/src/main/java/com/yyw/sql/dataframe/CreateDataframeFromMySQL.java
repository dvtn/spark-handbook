package com.yyw.sql.dataframe;

import org.apache.spark.sql.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CreateDataframeFromMySQL {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("createDataframefromMySQLApp")
                .getOrCreate();
        /**
         * 每一种方式读取MySQL数据库表
         */
        Map<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://localhost:3306/spark");
        options.put("driver", "com.mysql.cj.jdbc.Driver");
        options.put("user", "root");
        options.put("password", "Love88me");
        options.put("dbtable", "user");

        Dataset<Row> userDF = spark.read().format("jdbc").options(options).load();
        userDF.show();
        //注册视图,该视图相当于指针，不占用内存和存储,指该表指向数据库中的user表
        userDF.createOrReplaceTempView("spark_user");

        /**
         * 每二种方式读取mysql数据库
         */
        DataFrameReader reader = spark.read().format("jdbc");
        reader.option("url", "jdbc:mysql://localhost:3306/spark");
        reader.option("driver", "com.mysql.cj.jdbc.Driver");
        reader.option("user", "root");
        reader.option("password", "Love88me");
        reader.option("dbtable", "department");

        Dataset<Row> departmentDF = reader.load();
        departmentDF.show();
        departmentDF.createOrReplaceTempView("spark_department");
        departmentDF.show();


        /**
         * 自定义sql语句查询
         */
        Dataset<Row> sqlDF = spark.sql("select u.user_code, u.full_name, u.position, d.department_desc, d.company_email " +
                " from spark_user u, spark_department d " +
                " where u.department_id=d.department_id");
        sqlDF.show();

        /**
         * 将自定义查询的结果保存到数据表
         */
        String url = "jdbc:mysql://localhost:3306/spark";
        String table = "user_department";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "Love88me");


        /**
         * SaveMode:
         *      1.OverWrite: 覆盖
         *      2.Append:追加
         *      3.ErrorIfExists:如果存在就报错
         *      4.Ignore:如果存在就忽略
         */
        sqlDF.write().mode(SaveMode.Overwrite).jdbc(url, table, properties);
        System.out.println("--------数据成功保存到数据库------------------");


        spark.close();
    }
}
