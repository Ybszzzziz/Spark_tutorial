package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 8:30
 * */
object Spark02_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

//        val res: Int = rdd.reduce(_ + _)
//        println(res)
        // count : 数据源中数据的个数
        val cnt: Long = rdd.count()
        println(cnt)

        // first : 获取数据源中数据的第一个
        val res: Int = rdd.first()
        println(res)
        
        // 获取N个数据
        val takes: Array[Int] = rdd.take(3)
        println(takes.mkString(""))
        
        // 排序后取N个数据
        val rdd1: RDD[Int] = sc.makeRDD(List(1, 4, 3, 4))
        val orderTake: Array[Int] = rdd1.takeOrdered(3)
        println(orderTake.mkString(""))
        sc.stop()
    }
}
