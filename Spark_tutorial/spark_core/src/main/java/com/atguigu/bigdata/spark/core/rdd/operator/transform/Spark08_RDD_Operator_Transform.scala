package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark08_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(1,2,3,4, 5, 6), 2)
    // 扩大分区，但是不进行shuffle，没有意义
    // 所以如果想要实现扩大分区的效果需要使用shuffle
//    val value = rdd.coalesce(3, true)
    val value = rdd.repartition(3)
    value.saveAsTextFile("output")
    sc.stop()


  }

}
