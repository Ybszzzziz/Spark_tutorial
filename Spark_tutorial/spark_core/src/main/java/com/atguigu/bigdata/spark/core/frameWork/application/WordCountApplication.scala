package com.atguigu.bigdata.spark.core.frameWork.application

import com.atguigu.bigdata.spark.core.frameWork.common.TApplication
import com.atguigu.bigdata.spark.core.frameWork.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 20:19
 * */
object WordCountApplication extends App with TApplication{
    
    
    start(){
        val controller = new WordCountController()
        controller.dispatch()
    }
    
 
    
}
