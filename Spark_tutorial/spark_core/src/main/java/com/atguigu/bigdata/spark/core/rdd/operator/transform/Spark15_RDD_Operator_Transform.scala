package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark15_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)),
      2)

    // 存在函数柯里化，有两个参数列表
    // 1：传递一个参数吗，初始值
    //    主要用于当碰见第一个key的时候，和value进行分区内计算
    // 2：传递两个参数
    //   2._1 分区内计算规则
    //   2._2 分区间计算规则
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    sc.stop()



  }

}
