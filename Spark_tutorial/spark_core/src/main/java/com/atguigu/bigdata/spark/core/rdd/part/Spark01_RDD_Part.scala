package com.atguigu.bigdata.spark.core.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 15:53
 * */
object Spark01_RDD_Part {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partition")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[(String, String)] = sc.makeRDD(List(
            ("NBA", "xxxxxxxxx"),
            ("CBA", "xxxxxxxxx"),
            ("WNBA", "xxxxxxxxx"),
            ("NBA", "xxxxxxxxx")
        ), 3)
        val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
        
        partRDD saveAsTextFile "output"
        
        sc.stop()
    }
    
    // 自定义分区器
    class MyPartitioner extends Partitioner {
        
        // 分区数量
        override def numPartitions: Int = 3
        
        // 根据数据的key值 返回分区的索引 从0开始
        override def getPartition(key: Any): Int = {
            
            key match {
                case "NBA" => 0
                case "WNBA" => 1
                case _ => 2
            }
        }
    }
    
}
