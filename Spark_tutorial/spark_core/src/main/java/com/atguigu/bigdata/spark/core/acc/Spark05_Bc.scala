package com.atguigu.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yan
 * @create 2023-09-19 16:14
 * */
object Spark05_Bc {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("acc")
        val sc = new SparkContext(sparkConf)
        
        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),
            ("b", 2),
            ("c", 3)
        ))
        
        val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
        
        rdd1.map{
            case (w, c) => {
                val value: Int = map.getOrElse(w, 0)
                (w, (c, value))
            }
        }.collect().foreach(println)
        
//        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
//            ("a", 4),
//            ("b", 5),
//            ("c", 6)
//        ))
//
//        // join会导致数据量几何增长， 影响shuffle性能
//        val rdd3: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//
//        // ()
//
//        rdd3.collect().foreach(println)
        
        
        sc.stop()
    }
}
