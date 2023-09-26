package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 15:01
 * */
object Spark_03_Persist {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sc = new SparkContext(sparkConf)
        
        val list = List("hello scala", "hello saprk")
        val rdd: RDD[String] = sc.makeRDD(list)
        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = flatRDD.map(
            word => {
                println("********")
                (word, 1)
        }
        )
        sc.setCheckpointDir("cp")
        // checkPoint需要落盘
        // 作业执行完毕后不会被删除
        // 一般保存在hdfs中
        mapRDD.checkpoint()
        val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        resRDD.collect().foreach(println)
        println("******************")
        
        val resRDD1: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        resRDD1.collect().foreach(println)
        
        
        
        
        sc.stop()
    }
    
}
