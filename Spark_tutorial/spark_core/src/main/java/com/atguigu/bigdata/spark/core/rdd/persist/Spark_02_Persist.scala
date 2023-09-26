package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 15:01
 * */
object Spark_02_Persist {
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
        // cache 默认保存在内存中 如果要保存到磁盘文件，则需要再persist中给出
//        mapRDD.cache()
        // 触发action算子时才会进行缓存
        mapRDD.persist(StorageLevel.DISK_ONLY)
        val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        resRDD.collect().foreach(println)
        println("******************")
        
        val resRDD1: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
        resRDD1.collect().foreach(println)
        
        
        
        
        sc.stop()
    }
    
}
