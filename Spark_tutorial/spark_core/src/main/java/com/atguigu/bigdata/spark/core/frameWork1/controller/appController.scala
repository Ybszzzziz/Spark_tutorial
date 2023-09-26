package com.atguigu.bigdata.spark.core.frameWork1.controller

import com.atguigu.bigdata.spark.core.frameWork.common.TController
import com.atguigu.bigdata.spark.core.frameWork1.service.appService

/**
 * @author Yan
 * @create 2023-09-21 19:01
 * */
class appController extends TController{
    
    private val service = new appService()
    
    def dispatch(): Unit = {
        service.run()
    }
    
}
