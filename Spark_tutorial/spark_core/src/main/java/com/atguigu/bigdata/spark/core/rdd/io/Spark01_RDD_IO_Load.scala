package com.atguigu.bigdata.spark.core.rdd.io

import org.apache.spark.api.java.JavaSparkContext.fromSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 16:04
 * */
object Spark01_RDD_IO_Load {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("IO")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[String] = sc.textFile("out")
        println(rdd.collect().mkString(","))
        
        val objRDD: RDD[(String, Int)] = sc.objectFile[(String, Int)]("out1")
        println(objRDD.collect().mkString(","))
        
        val seqRDD: RDD[(String, Int)] = sc.sequenceFile[String, Int]("out2")
        println(seqRDD.collect().mkString(","))
        
        sc.stop()
    }
    
}
