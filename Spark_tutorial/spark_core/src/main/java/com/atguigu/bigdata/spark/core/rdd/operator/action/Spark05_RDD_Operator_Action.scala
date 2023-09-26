package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 8:30
 * */
object Spark05_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

//        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("a", 3)
        ))
//        rdd.collect().foreach()
        sc.stop()
    }
}
