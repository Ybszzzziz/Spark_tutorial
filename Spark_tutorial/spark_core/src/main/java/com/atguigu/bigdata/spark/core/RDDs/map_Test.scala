package com.atguigu.bigdata.spark.core.RDDs

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-21 21:53
 * */
object map_Test {
  def main(args: Array[String]): Unit = {
    val sparkCon = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(sparkCon)
    val lines = sc.textFile("datas/1.txt")
    val l = lines.count()
    val rdd_1 = lines.map(_.length)
    val ints = rdd_1.take(2)
    ints.foreach(println)
    sc.stop();
  }

}
