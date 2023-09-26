package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 16:14
 * */
object Spark01_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//        val res: Int = rdd.reduce(_ + _)
//        println(res)
        var sum = 0
        rdd.collect().foreach(sum += _)
        println(sum)
        
        sc.stop()
    }
    
}
