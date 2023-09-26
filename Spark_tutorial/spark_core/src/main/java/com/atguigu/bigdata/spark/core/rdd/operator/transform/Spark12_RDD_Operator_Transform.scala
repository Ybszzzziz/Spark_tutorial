package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mapRDD = rdd.map((_, 1))
    // RDD => PairRDDFunctions
    // 隐式转换 （二次编译）
    mapRDD.partitionBy(new HashPartitioner(2)).saveAsTextFile("output")
    sc.stop()


  }

}
