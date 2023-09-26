package com.atguigu.bigdata.spark.core.frameWork1.common

import com.atguigu.bigdata.spark.core.frameWork.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-21 18:31
 * */
trait TApp {
    
    def start(ApplicationMaster: String="local[*]",AppName: String="Application")(method: => Unit): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster(ApplicationMaster).setAppName(AppName)
        val sc = new SparkContext(sparkConf)
        EnvUtil.put(sc)
        try {
            method
        } catch {
            case ex => println(ex.getMessage)
        }
        sc.stop()
        EnvUtil.clear()
    }
}
