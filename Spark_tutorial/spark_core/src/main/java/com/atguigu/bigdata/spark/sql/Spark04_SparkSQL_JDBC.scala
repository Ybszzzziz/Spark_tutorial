package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

/**
 * @author Yan
 * @create 2023-09-25 10:32
 * */
object Spark04_SparkSQL_JDBC {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        
        // 读取mysql数据
        spark.read.format("jdbc")
            .option("url", "jdbc:mysql://8.130.141.56:3306/wordpress")
            .option("driver",  "com.mysql.jdbc.Driver")
            .option("user", "root")
            .option("password", "Ybs123123.")
            .option("dbtable","wp_users").load().show
        
        spark.close()
        
        
    }
    
    
}

