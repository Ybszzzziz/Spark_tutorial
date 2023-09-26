package com.atguigu.bigdata.spark.core.frameWork1.common

import com.atguigu.bigdata.spark.core.frameWork.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author Yan
 * @create 2023-09-21 19:37
 * */
trait TDao {
    
    def getFile(path: String): RDD[String] = {
        EnvUtil.get().textFile(path)
    }
    
}
