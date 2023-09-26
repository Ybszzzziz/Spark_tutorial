package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-18 18:45
 * */
object case1 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
        val sc = new SparkContext(sparkConf)
        val orginRDD: RDD[String] = sc.textFile("datas/agent.log")
        val mapRDD: RDD[((String, String), Int)] = orginRDD.map(
            line => {
                val data: Array[String] = line.split(" ")
                ((data(1), data(4)), 1)
            }
        )
        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
        val newRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            case ((province, adv), sum) => {
                (province, (adv, sum))
            }
        }
        val grpRDD: RDD[(String, Iterable[(String, Int)])] = newRDD.groupByKey()
        val resRDD: RDD[(String, List[(String, Int)])] = grpRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )
        resRDD.collect().foreach(println)
    }

}
