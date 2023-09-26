package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 8:57
 * */
object Spark06_Req3_PageFlowAnalysis {
    
    def main(args: Array[String]): Unit = {
    
        // TODO: top10
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        
        val mapRDD: RDD[UserVisitAction] = rdd.map(
            action => {
                val datas: Array[String] = action.split("_")
                UserVisitAction(
                    datas(0),
                    datas(1).toLong,
                    datas(2),
                    datas(3).toLong,
                    datas(4),
                    datas(5),
                    datas(6).toLong,
                    datas(7).toLong,
                    datas(8),
                    datas(9),
                    datas(10),
                    datas(11),
                    datas(12).toLong
                )
            }
        )
        mapRDD.cache()
        
        // 指定页面
        
        val ids: List[Long] = List[Long](1, 2, 3, 4, 5, 6, 7)
        val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)
        
        val map: Map[Long, Long] = mapRDD.filter(
            action => {
                // init 不包含最后一个
                ids.init.contains(action.page_id)
            }
        ).map(
            action => {
                (action.page_id, 1L)
            }
        ).reduceByKey(_ + _).collect().toMap
        
        // 计算分子
        val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = mapRDD.groupBy(_.session_id)
        val zipRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
            iter => {
                val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
                val flowIds: List[Long] = sortList.map(_.page_id)
                val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
                
                // 将不合法的页面跳转进行过滤
                pageflowIds.filter(okflowIds.contains(_)).map((_, 1))
            }
        )
        val flatRDD: RDD[((Long, Long), Int)] = zipRDD.map(_._2).flatMap(x => x)
        
        val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)
        
        // 计算单跳转换率
        dataRDD.foreach{
            case ((pageid1, pageid2), sum) => {
                val l: Long = map.getOrElse(pageid1, 0L)
                println(s"页面${pageid1}跳转到页面${pageid2}的转换率为" + (sum.toDouble / l))
            }
        }
        
        
        
        
        sc.stop()
    }
    
    case class UserVisitAction(
              date: String, //用户点击行为的日期
              user_id: Long, //用户的 ID
              session_id: String, //Session 的 ID
              page_id: Long, //某个页面的 ID
              action_time: String, //动作的时间点
              search_keyword: String, //用户搜索的关键词
              click_category_id: Long, //某一个商品品类的 ID
              click_product_id: Long, //某一个商品的 ID
              order_category_ids: String, //一次订单中所有品类的 ID 集合
              order_product_ids: String, //一次订单中所有商品的 ID 集合
              pay_category_ids: String, //一次支付中所有品类的 ID 集合
              pay_product_ids: String, //一次支付中所有商品的 ID 集合
              city_id: Long //城市 id
                              )
    
}
