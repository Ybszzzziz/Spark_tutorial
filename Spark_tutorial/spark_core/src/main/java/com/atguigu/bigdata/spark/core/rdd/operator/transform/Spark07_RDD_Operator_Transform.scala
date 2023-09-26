package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark07_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4, 5, 6), 3)
    // 这种情况下缩减分区，可能会导致数据不均衡，发生数据倾斜
    // 可以使用shuffle
    val value = rdd.coalesce(2, true)
    value.saveAsTextFile("output")

  }

}
