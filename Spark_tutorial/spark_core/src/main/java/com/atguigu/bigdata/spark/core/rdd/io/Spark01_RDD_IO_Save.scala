package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 16:04
 * */
object Spark01_RDD_IO_Save {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("IO")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3)
        ))
        rdd.saveAsTextFile("out")
        rdd.saveAsObjectFile("out1")
        rdd.saveAsSequenceFile("out2")
        
        sc.stop()
    }
    
}
