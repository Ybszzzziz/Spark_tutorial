package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 8:30
 * */
object Spark03_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        
        // 初始值即参与分区内计算，又参与分区间计算
        println(rdd.aggregate(10)(_ + _, _ + _))
        
        println(rdd.fold(10)(_ + _))
        
        sc.stop()
    }
}
