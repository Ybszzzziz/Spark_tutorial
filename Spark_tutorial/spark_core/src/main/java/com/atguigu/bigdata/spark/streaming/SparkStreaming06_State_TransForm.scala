package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming06_State_TransForm {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        
        // transform方法可以将底层rdd获取到后进行操作
        // 1. DStream不好操作功能不完善
        // 2. 需要RDD周期性执行
        val rdd: DStream[String] = lines.transform(rdd => rdd)
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    
}
