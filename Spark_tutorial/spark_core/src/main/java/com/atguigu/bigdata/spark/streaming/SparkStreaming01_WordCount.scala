package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming01_WordCount {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        
        // TODO 逻辑处理
        // 获取端口数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
        
        
        // TODO 关闭环境
        // 由于要长期采集，所以不能直接关闭
        // 如果main方法执行完毕 程序也会结束，所以不能让main方法执行挖鼻
//        ssc.stop()
        // 1.启动采集器
        ssc.start()
        
        // 2.等待采集器的关闭
        ssc.awaitTermination()
    }
    
}
