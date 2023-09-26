package com.atguigu.bigdata.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-22 11:04
 * */
object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    // 从内存中创建RDD
    val seq = Seq[Int](1, 2, 3, 4)

//    val rdd = sc.parallelize(seq)
//    val rdd = sc.makeRDD(seq)
//    val rdd = sc.textFile("hdfs://hadoop102:8020/")
    val rdd = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)

    sc.stop()


  }
}
