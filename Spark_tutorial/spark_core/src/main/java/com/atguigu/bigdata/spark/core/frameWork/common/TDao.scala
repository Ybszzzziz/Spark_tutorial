package com.atguigu.bigdata.spark.core.frameWork.common

import com.atguigu.bigdata.spark.core.frameWork.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
 * @author Yan
 * @create 2023-09-20 20:40
 * */
trait TDao {
    
    def readFile(path: String): RDD[String] = {
        EnvUtil.get().textFile(path)
    }
    
}
