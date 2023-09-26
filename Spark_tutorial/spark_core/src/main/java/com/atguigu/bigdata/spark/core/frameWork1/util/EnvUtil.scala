package com.atguigu.bigdata.spark.core.frameWork1.util

import org.apache.spark.SparkContext

/**
 * @author Yan
 * @create 2023-09-21 19:45
 * */
object EnvUtil {
    
    private val scEnv = new ThreadLocal[SparkContext]()
    
    def put(sc: SparkContext): Unit = {
        scEnv.set(sc)
    }
    
    def get(): SparkContext = {
        scEnv.get()
    }
    
    def clear(): Unit = {
        scEnv.remove()
    }
    
}
