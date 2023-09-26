package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * @author Yan
 * @create 2023-09-25 10:32
 * */
object Spark05_SparkSQL_HIVE {
    
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val spark: SparkSession = SparkSession.builder().config("hive.metastore.uris", "thrift://hadoop102:9083").enableHiveSupport().master("local[*]").
                appName("spark-sql").getOrCreate()
        // 使用sparksql 连接外置hive
        // 1.拷贝Hive-site.xml 到classpath
        // 2.启动Hive的支持
        // 3.增加对应的依赖关系（包含mysql的驱动）
        spark.sql(
            """
              |CREATE TABLE `user_visit_action`(
              | `date` string,
              | `user_id` bigint,
              | `session_id` string,
              | `page_id` bigint,
              | `action_time` string,
              | `search_keyword` string,
              | `click_category_id` bigint,
              | `click_product_id` bigint,
              | `order_category_ids` string,
              | `order_product_ids` string,
              | `pay_category_ids` string,
              | `pay_product_ids` string,
              | `city_id` bigint)
              |row format delimited fields terminated by '\t'
            """.stripMargin)
        spark.sql(
            """
              |load data local inpath 'datas/user_visit_action.txt' into table
              |user_visit_action;
              |""".stripMargin)
        spark.sql(
            """
              |CREATE TABLE `product_info`(
              | `product_id` bigint,
              | `product_name` string,
              | `extend_info` string)
              |row format delimited fields terminated by '\t';
              |""".stripMargin)
        spark.sql(
            """
              |load data local inpath 'datas/product_info.txt' into table product_info;
              |""".stripMargin)
        spark.sql(
            """
              |CREATE TABLE `city_info`(
              | `city_id` bigint,
              | `city_name` string,
              | `area` string)
              |row format delimited fields terminated by '\t';
            """.stripMargin)
        
        spark.sql(
            """
              |load data local inpath 'datas/city_info.txt' into table city_info;
            """.stripMargin)
        
        spark.sql("select * from user_visit_action").show()
        spark.close()
        
        
    }
    
    
}

