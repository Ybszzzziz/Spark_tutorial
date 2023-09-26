package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-21 11:04
 * */
object Spark01_WordCount {
  
  def main(args: Array[String]): Unit = {


    // 建立和spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    // TODO 执行业务操作
    val lines: RDD[String] = sc.textFile("datas")
    val words = lines.flatMap(x => x.split(" "))
    val wordGroup = words.groupBy(word => word)
    val value = wordGroup.map { case (word, list) => (word, list.size) }
    val tuples = value.collect()
    tuples.foreach(println)

    // TODO 关闭连接
    sc.stop()
  }

}
