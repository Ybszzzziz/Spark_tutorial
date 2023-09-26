package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))
    // 交集
    val intersectRDD = rdd1.intersection(rdd2)
    println(intersectRDD.collect().mkString(","))

    // 并集
    val unionRDD = rdd1.union(rdd2)
    println(unionRDD.collect().mkString(","))

    // 差集
    val subRDD1 = rdd1.subtract(rdd2)
    println(subRDD1.collect().mkString(","))

    // 拉链
    val value = rdd1.zip(rdd2)
    println(value.collect().mkString(","))

    sc.stop()


  }

}
