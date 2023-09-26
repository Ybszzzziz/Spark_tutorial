package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 8:57
 * */
object Spark01_Req1_HotCategoryTop10Analysis {
    
    def main(args: Array[String]): Unit = {
    
        // TODO: top10
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        
        // 1.读取原始日志数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        
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
        
        val resRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickResRDD.
                cogroup(odResRDD, payResRDD)
        val mergeRDD: RDD[(String, (Int, Int, Int))] = resRDD.mapValues {
            case (click, od, pay) => {
                var clickCnt = 0
                val iter1 = click.iterator
                if (iter1.hasNext) {
                    clickCnt = iter1.next()
                }
                var odCnt = 0
                val iter2 = od.iterator
                if (iter2.hasNext) {
                    odCnt = iter2.next()
                }
                var payCnt = 0
                val iter3 = pay.iterator
                if (iter3.hasNext) {
                    payCnt = iter3.next()
                }
                (clickCnt, odCnt, payCnt)
            }
        }
        
        val res: Array[(String, (Int, Int, Int))] = mergeRDD.sortBy(_._2, false).take(10)
        
        // 6.将结果采集到控制台
        res.foreach(println)
        
        sc.stop()
    }
}
