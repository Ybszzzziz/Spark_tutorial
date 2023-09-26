package com.atguigu.bigdata.spark.core.frameWork.common

import com.atguigu.bigdata.spark.core.frameWork.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-20 20:33
 * */
trait TApplication {
    
    def start(master: String="local[*]", app: String="Application")(op: => Unit): Unit = {
        val sparkConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparkConf)
        EnvUtil.put(sc)
        try{
            op
        } catch {
            case ex => println(ex.getMessage)
        }
        sc.stop()
        EnvUtil.clear()
    }
    
}
