package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yan
 * @create 2023-09-19 16:14
 * */
object Spark04_Acc_WordCount {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[String] = sc.makeRDD(List("hello", "world", "scala", "spark"))
        
        // 创建累加器对象向spark进行注册
        val wcAcc = new MyAccumulator()
        
        // 向Spark进行注册
        sc.register(wcAcc, "wordCountAcc")
        
        rdd.foreach(
            word => {
                wcAcc.add(word)
            }
        )
        
        // 获取累加器结果
        println(wcAcc.value)
        
        sc.stop()
    }
    
    /**
     *  1. 继承AccumulatorV2， 定义泛型
     *  IN ：累加器输入的数据类型
     *  OUT：累加器返回的数据类型
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {
        
        private var wcMap = mutable.Map[String, Long]()
        
        // 判断是否为初始状态
        override def isZero: Boolean = {
            wcMap.isEmpty
        }
        
        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAccumulator()
        }
        
        override def reset(): Unit = wcMap.clear()
        
        override def add(v: String): Unit = {
            
            val newVal: Long = wcMap.getOrElse(v, 0L) + 1L
            wcMap.update(v, newVal)
        }
        
        // Driver合并多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            
            val map1 = this.wcMap
            val map2 = other.value
            
            map2.foreach {
                case (word, count) => {
                    val newVal: Long = map1.getOrElse(word, 0L) + count
                    map1.update(word, newVal)
                }
            }
            
        }
        
        // 累加器结果
        override def value: mutable.Map[String, Long] = wcMap
    }
    
    
    
}
