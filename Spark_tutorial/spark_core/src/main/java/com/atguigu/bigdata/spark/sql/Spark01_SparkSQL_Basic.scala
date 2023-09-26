package com.atguigu.bigdata.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-25 10:32
 * */
object Spark01_SparkSQL_Basic {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        
        val df: DataFrame = spark.read.json("datas/user.json")
        //df.show
//        df.createOrReplaceTempView("user")
//        spark.sql("select * from user").show
//        spark.sql("select avg(age) from user").show
        
//        df.select("age", "username").show
        import spark.implicits._
//        df.select($"age" + 1, $"username").show
        
//        val seq = Seq(1, 2, 3, 4)
//        val ds: Dataset[Int] = seq.toDS()
//        ds.show
        val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 40), (2, "sili", 30)))
        val df1: DataFrame = rdd.toDF("id", "name", "age")
        val rdd1: RDD[Row] = df1.rdd
        
        val ds: Dataset[User] = df1.as[User]
        ds.toDF
        
        
        spark.close()
        
        
    }
    
    case class User(id: Int, name: String, age: Int)
    
}
