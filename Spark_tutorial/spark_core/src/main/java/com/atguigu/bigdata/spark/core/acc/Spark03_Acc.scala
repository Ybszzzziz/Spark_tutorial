package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 16:14
 * */
object Spark03_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
        
        // 获取系统累加器
        val sum: LongAccumulator = sc.longAccumulator("sum")
        val mapRDD: RDD[Int] = rdd.map(
            num => {
                sum.add(num)
                num
            }
        )
        
        // 获取累加器的值
        // 类似于 cache
        // 少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
        // 多加：
        // 一般情况下，累加器会放置在行动算子中进行操作
        mapRDD.collect()
        
        println(sum.value)
        
        sc.stop()
    }
    
}
