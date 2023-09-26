package com.atguigu.bigdata.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 14:06
 * */
object Spark02_RDD_Dep {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sc = new SparkContext(sparkConf)
        val rdd: RDD[String] = sc.textFile("datas/word.txt")
        println(rdd.dependencies)
        println("******************")
        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
        println(flatRDD.dependencies)
        println("******************")
        val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
        println(mapRDD.dependencies)
        println("******************")
        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        println(reduceRDD.dependencies)
        println("******************")
        reduceRDD.collect().foreach(println)
        sc.stop()
        
    }
    
}
