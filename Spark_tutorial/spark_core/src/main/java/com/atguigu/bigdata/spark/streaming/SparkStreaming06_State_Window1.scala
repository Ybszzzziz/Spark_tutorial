    package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming06_State_Window1 {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        
        val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
        val mapDS: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
            (x: Int, y: Int) => {
                x + y
            },
            (x: Int, y: Int) => {
                x - y
            },
            Seconds(9), Seconds(3)
        )
        
        mapDS.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    
}
