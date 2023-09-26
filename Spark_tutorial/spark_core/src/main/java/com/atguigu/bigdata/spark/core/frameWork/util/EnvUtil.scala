package com.atguigu.bigdata.spark.core.frameWork.util

import org.apache.spark.SparkContext

/**
 * @author Yan
 * @create 2023-09-20 20:45
 * */
object EnvUtil {
    
    private val scLocal = new ThreadLocal[SparkContext]()
    
    def put(sc: SparkContext): Unit = {
        scLocal.set(sc)
    }
    
    def get(): SparkContext = {
        scLocal.get()
    }
    
    def clear(): Unit = {
        scLocal.remove()
    }
    
    
}
