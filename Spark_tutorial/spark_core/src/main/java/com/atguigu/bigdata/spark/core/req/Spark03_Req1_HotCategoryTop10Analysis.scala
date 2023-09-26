package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 8:57
 * */
object Spark03_Req1_HotCategoryTop10Analysis {
    
    def main(args: Array[String]): Unit = {
    
        // TODO: top10
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        
        // TODO: optimize
        // 存在大量的shuffle （reduceByKey）
        // reduceByKey聚合算子，spark会优化 缓存
        
        // 1.读取原始日志数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        
        // 2.数据转换结构
        //   点击的场合：（品类ID， （1，0，0））
        //   下单的场合：（品类ID， （0，1，0））
        //   支付的场合：（品类ID， （0，0，1））
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
        
        val res: Array[(String, (Int, Int, Int))] = mergeRDD.sortBy(_._2, false).take(10)

        // 6.将结果采集到控制台
        res.foreach(println)
        
        sc.stop()
    }
}
