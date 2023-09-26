package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming06_State_Join {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        val data9999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        val data8888: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)
        
        val map9999: DStream[(String, Int)] = data9999.map((_, 9))
        val map8888: DStream[(String, Int)] = data8888.map((_, 8))
        
        // join操作就是两个rdd的join
        val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)
        
        joinDS.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    
}
