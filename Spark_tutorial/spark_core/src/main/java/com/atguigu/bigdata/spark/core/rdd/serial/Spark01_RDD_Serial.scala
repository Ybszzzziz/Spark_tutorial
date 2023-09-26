package com.atguigu.bigdata.spark.core.rdd.serial

import com.atguigu.bigdata.spark.core.rdd.serial.Spark01_RDD_Serial.Search
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 11:12
 * */
object Spark01_RDD_Serial {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
        val sc = new SparkContext(sparkConf)
        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))
        
        val search = new Search("h")
        search.getMatch1(rdd).collect().foreach(println)
        
        sc.stop()
    }
    // 类的构造参数其实是类的属性
    class Search(query: String) extends Serializable {
        
        def isMatch(s: String): Boolean = {
            s.contains(query)
        }
        
        def getMatch1(rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }
        
        def getMatch2(rdd: RDD[String]): RDD[String] = {
            rdd.filter(_.contains(query))
        }
        
    }
    
}
