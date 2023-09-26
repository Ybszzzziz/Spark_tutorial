package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yan
 * @create 2023-09-19 16:14
 * */
object Spark06_Bc {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3)
        ))
        
        val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
        
        // 封装广播变量
        val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)
        
        // 获取
        rdd1.map {
            case (w, c) => {
                
                // 访问广播变量
                val value: Int = bc.value.getOrElse(w, 0)
                (w, (c, value))
            }
        }.collect().foreach(println)
        
        
        sc.stop()
    }
}
