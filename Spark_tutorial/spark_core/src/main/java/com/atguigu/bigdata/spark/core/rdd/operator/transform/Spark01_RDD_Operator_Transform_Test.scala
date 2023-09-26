package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("datas/apache.log")
    val rdd1 = rdd.map(
      line => {
        val strings = line.split(" ")
        strings(6)
      }
    )
    rdd1.collect().foreach(println)


  }

}
