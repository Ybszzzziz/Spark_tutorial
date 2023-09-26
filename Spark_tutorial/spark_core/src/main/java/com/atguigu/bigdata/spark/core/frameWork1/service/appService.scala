package com.atguigu.bigdata.spark.core.frameWork1.service

import com.atguigu.bigdata.spark.core.frameWork1.bean.UserVisitActionBean
import com.atguigu.bigdata.spark.core.frameWork1.common.TService
import com.atguigu.bigdata.spark.core.frameWork1.dao.appDao
import com.atguigu.bigdata.spark.core.req.Spark06_Req3_PageFlowAnalysis.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.lang.reflect.Field

/**
 * @author Yan
 * @create 2023-09-21 19:01
 * */
class appService extends TService{
    
    private val dao = new appDao()
    
    def run(): Unit = {
        // TODO: top10
        
        val rdd: RDD[String] = dao.getFile("datas/user_visit_action.txt")
        
        val mapRDD: RDD[UserVisitActionBean] = rdd.map(
            action => {
                val datas: Array[String] = action.split("_")
                val bean = new UserVisitActionBean()
                bean.setDate(datas(0))
                bean.setUser_id(datas(1).toLong)
                bean.setSession_id(datas(2))
                bean.setPage_id(datas(3).toLong)
                bean.setAction_time(datas(4))
                bean.setSearch_keyword(datas(5))
                bean.setClick_category_id(datas(6).toLong)
                bean.setClick_product_id(datas(7).toLong)
                bean.setOrder_category_ids(datas(8))
                bean.setOrder_product_ids(datas(9))
                bean.setPay_category_ids(datas(10))
                bean.setPay_product_ids(datas(11))
                bean.setCity_id(datas(12).toLong)
                bean
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
        val sessionRDD: RDD[(String, Iterable[UserVisitActionBean])] = mapRDD.groupBy(_.session_id)
        val zipRDD: RDD[(String, List[((Long, Long), Int)])] = sessionRDD.mapValues(
            iter => {
                val sortList: List[UserVisitActionBean] = iter.toList.sortBy(_.action_time)
                val flowIds: List[Long] = sortList.map(_.page_id)
                val pageflowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
                
                // 将不合法的页面跳转进行过滤
                pageflowIds.filter(okflowIds.contains(_)).map((_, 1))
            }
        )
        val flatRDD: RDD[((Long, Long), Int)] = zipRDD.map(_._2).flatMap(x => x)
        
        val dataRDD: RDD[((Long, Long), Int)] = flatRDD.reduceByKey(_ + _)
        
        // 计算单跳转换率
        dataRDD.foreach {
            case ((pageid1, pageid2), sum) => {
                val l: Long = map.getOrElse(pageid1, 0L)
                println(s"页面${pageid1}跳转到页面${pageid2}的转换率为" + (sum.toDouble / l))
            }
        }
    }

}
