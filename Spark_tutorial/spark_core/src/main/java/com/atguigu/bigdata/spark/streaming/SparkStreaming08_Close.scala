    package com.atguigu.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
    
    /**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
    object SparkStreaming08_Close {
    
    def main(args: Array[String]): Unit = {
        
        /**
         * 线程的关闭：
         * val thread = new Thread()
         * thread.start()
         * thread.stop() 强制关闭
         */
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        
        val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
        wordToOne.print()
        
//        mapDS.print()
        ssc.start()
        
        // 如果要关闭采集器，那么需要创建新的线程
        // 而且在第三方程序中增加关闭状态
        new Thread(
            new Runnable {
                override def run(): Unit = {
                    // 优雅的关闭
                    // 计算节点不在接受新的数据，er'shi'jiang'xian'you'de'shu
//                    while (true) {
//                        if (true) {
//                            val state: StreamingContextState = ssc.getState()
//                            if (state == StreamingContextState.ACTIVE) {
//                                ssc.stop(true, true)
//                            }
//                        }
//                        Thread.sleep(5000)
//                    }
                    Thread.sleep(5000)
                    val state: StreamingContextState = ssc.getState()
                    if (state == StreamingContextState.ACTIVE) {
                        ssc.stop(true, true)
                    }
                    System.exit(0)
                }
            }
        ).start()
        
        ssc.awaitTermination()
       
    }
    
    
    
}
