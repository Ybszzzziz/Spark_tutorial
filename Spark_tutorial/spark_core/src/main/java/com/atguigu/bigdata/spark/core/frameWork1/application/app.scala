package com.atguigu.bigdata.spark.core.frameWork1.application

import com.atguigu.bigdata.spark.core.frameWork1.common.TApp
import com.atguigu.bigdata.spark.core.frameWork1.controller.appController

/**
 * @author Yan
 * @create 2023-09-21 18:26
 * */
object app extends App with TApp{
    
    private val controller = new appController()
    start()(
        controller.dispatch()
    )
    
}
