package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 8:57
 * */
object Spark05_Req2_HotCategoryTop10_SessionAnalysis {
    
    def main(args: Array[String]): Unit = {
    
        // TODO: top10
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        
        val top10_Ids: Array[String] = top10Category(rdd)
        
        // 1.过滤原始数据, 保留前十品类id和点击
        rdd.cache()
        val filterRDD: RDD[String] = rdd.filter(
            action => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    top10_Ids.contains(datas(6))
                } else {
                    false
                }
            }
        )
        
        // 2.根据品类ID和SessionId进行点击量的统计
        val reduceRDD: RDD[((String, String), Int)] = filterRDD.map(
            action => {
                val datas: Array[String] = action.split("_")
                ((datas(6), datas(2)), 1)
            }
        ).reduceByKey(_ + _)
        
        // 3.统计结果结构转换
        val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
            case ((cid, seid), cnt) => {
                (cid, (seid, cnt))
            }
        }
        
        // 4.相同品类进行分组
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
        
        // 5.将分组后的数据
        val res: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
            }
        )
        
        res.collect().foreach(println)
        
        sc.stop()
    }
    
    def top10Category(rdd: RDD[String]): Array[String] = {
        
        val flagRDD: RDD[(String, (Int, Int, Int))] = rdd.flatMap {
            action => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击
                    List((datas(6), (1, 0, 0)))
                } else if (datas(8) != "null") {
                    // 下单
                    val ids: Array[String] = datas(8).split(",")
                    ids.map((_, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    // 支付
                    val ids: Array[String] = datas(10).split(",")
                    ids.map((_, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        }
        
        // 3.将相同品类ID 的数据进行分组聚合
        // （品类ID, (点击数量，下单数量，支付数量)）
        val mergeRDD: RDD[(String, (Int, Int, Int))] = flagRDD.reduceByKey {
            case (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        }
        // 4.统计结果根据数量降序排序 取前10
        
        val res: Array[String] = mergeRDD
                .sortBy(_._2, false).
                take(10).map(_._1)
        res
    }
    
}
