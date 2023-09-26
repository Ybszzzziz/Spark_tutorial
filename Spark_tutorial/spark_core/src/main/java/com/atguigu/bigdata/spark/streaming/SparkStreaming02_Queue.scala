package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming02_Queue {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        
        val rddQueue = new mutable.Queue[RDD[Int]]()
        
        val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, false)
        
        inputStream.map((_, 1))
            .reduceByKey(_ + _)
            .print()
        
        // 1.启动采集器
        ssc.start()
        
        for (i <- 1 to 5) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }
        
        // 2.等待采集器的关闭
        ssc.awaitTermination()
    }
    
}
