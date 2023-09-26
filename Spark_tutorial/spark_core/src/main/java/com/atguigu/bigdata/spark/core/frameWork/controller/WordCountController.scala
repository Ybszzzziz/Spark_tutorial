package com.atguigu.bigdata.spark.core.frameWork.controller

import com.atguigu.bigdata.spark.core.frameWork.common.TController
import com.atguigu.bigdata.spark.core.frameWork.dao.WordCountDao
import com.atguigu.bigdata.spark.core.frameWork.service.WordCountService

/**
 * @author Yan
 * @create 2023-09-20 20:20
 * 控制层
 * */
class WordCountController extends TController{
    
    private val wordCountService = new WordCountService()
    
    def dispatch()= {
        val res: Array[(String, Int)] = wordCountService.dataAnalyze()
        res.foreach(println)
    }
    
}
