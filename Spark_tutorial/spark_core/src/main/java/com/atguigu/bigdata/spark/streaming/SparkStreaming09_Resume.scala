    package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
    
    /**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
    object SparkStreaming09_Resume {
    
    def main(args: Array[String]): Unit = {
        
        val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
            // TODO 创建环境对象
            val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
            val ssc = new StreamingContext(sparkConf, Seconds(3))
            val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
            
            val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
            wordToOne.print()
            ssc
        })
        
        
        
        ssc.start()
        ssc.awaitTermination()
       
    }
    
    
    
}
