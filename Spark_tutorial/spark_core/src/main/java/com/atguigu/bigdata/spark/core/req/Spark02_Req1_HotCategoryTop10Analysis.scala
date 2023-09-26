package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 8:57
 * */
object Spark02_Req1_HotCategoryTop10Analysis {
    
    def main(args: Array[String]): Unit = {
    
        // TODO: top10
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        
        // TODO: optimize
        // rdd重复使用
        // cogroup性能低
        
        // 1.读取原始日志数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        rdd.cache()
        
        // 2.统计品类的点击数量，（品类ID， 点击数量）
        val clickRDD: RDD[String] = rdd.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(6) != "-1"
            }
        )
        val clickResRDD: RDD[(String, Int)] = clickRDD.map(
            action => {
                val datas: Array[String] = action.split("_")
                (datas(6), 1)
            }
        ).reduceByKey(_ + _)
        
        // 3.统计品类的下单数量，（品类ID， 点击数量）
        val orderRDD: RDD[String] = rdd.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(8) != "null"
            }
        )
        val odResRDD: RDD[(String, Int)] = orderRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                val cids: Array[String] = datas(8).split(",")
                cids.map((_, 1))
            }
        ).reduceByKey(_+_)
        
        // 4.统计品类的支付数量，（品类ID， 点击数量）
        val payRDD: RDD[String] = rdd.filter(
            action => {
                val datas: Array[String] = action.split("_")
                datas(10) != "null"
            }
        )
        val payResRDD: RDD[(String, Int)] = payRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                val cids: Array[String] = datas(10).split(",")
                cids.map((_, 1))
            }
        ).reduceByKey(_ + _)
        
        // 5.将品类进行排序，并且取前十名
        // 点击数量排序，下单数量排序，支付数量排序
        // 元组排序：先比较第一个。。。第二个。。第三个
        // cogroup 有可能存在shuffle
        val rdd1: RDD[(String, (Int, Int, Int))] = clickResRDD.map {
            case (cid, cnt) => {
                (cid, (cnt, 0, 0))
            }
        }
        
        val rdd2: RDD[(String, (Int, Int, Int))] = odResRDD.map {
            case (cid, cnt) => {
                (cid, (0, cnt, 0))
            }
        }
        
        val rdd3: RDD[(String, (Int, Int, Int))] = payResRDD.map {
            case (cid, cnt) => {
                (cid, (0, 0, cnt))
            }
        }
        
        // 将三个数据源合并在一起，统一进行聚合计算
        val unionRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
        val mergeRDD: RDD[(String, (Int, Int, Int))] = unionRDD.reduceByKey {
            case (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        }
        
        
        val res: Array[(String, (Int, Int, Int))] = mergeRDD.sortBy(_._2, false).take(10)

        // 6.将结果采集到控制台
        res.foreach(println)
        
        sc.stop()
    }
}
