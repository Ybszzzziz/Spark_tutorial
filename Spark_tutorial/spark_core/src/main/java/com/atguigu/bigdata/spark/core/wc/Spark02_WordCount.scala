package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-21 11:04
 * */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    // application
    // spark框架
    // TODO建立和spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile("datas")
    val words = lines.flatMap(x => x.split(" "))
    val wordToOne = words.map(word => (word, 1))
    val value = wordToOne.groupBy(x => x._1)
    val value1 = value.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => (t1._1, t1._2 + t2._2)
        )
      }
    }
    value1.foreach(println)


    // TODO执行业务操作

    // TODO 关闭连接
    sc.stop()
  }

}
