package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 16:14
 * */
object Spark02_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        
        // 获取系统累加器
        val sum: LongAccumulator = sc.longAccumulator("sum")
        rdd.foreach(sum.add(_))
        println(sum.value)
        
        sc.stop()
    }
    
}
