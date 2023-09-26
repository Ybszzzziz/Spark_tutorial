package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark09_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("11", 1), ("22", 3), ("8", 65)), 2)
    // 默认升序
    val value = rdd.sortBy(num => num._1, false)
    value.saveAsTextFile("output")
    sc.stop()


  }

}
