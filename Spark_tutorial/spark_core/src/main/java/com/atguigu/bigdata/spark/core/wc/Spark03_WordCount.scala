package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-21 11:04
 * */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    // application
    // spark框架
    // TODO建立和spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("datas")
    val words = lines.flatMap(x => x.split(" "))
    val wordToOne = words.map(word => (word, 1))
    // spark 框架提供的功能
//    wordToOne.reduceByKey((x, y) => (x + y))
    val wordToCount = wordToOne.reduceByKey(_ + _)
    wordToCount.foreach(println)


    // TODO执行业务操作

    // TODO 关闭连接
    sc.stop()
  }

}
