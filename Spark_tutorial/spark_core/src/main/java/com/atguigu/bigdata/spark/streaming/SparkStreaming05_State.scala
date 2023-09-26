package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming05_State {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        ssc.checkpoint("cp")
        
        // 无状态数据操作，只对当前的采集周期内的数据进行处理
        // 在某些场合下，需要保留数据统计结果（重点），实现数据的汇总
        // 使用有状态操作时，需要设置检查点
        val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
        
        val wordToOne: DStream[(String, Int)] = datas.map((_, 1))
        
//        val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
        // 根据key对数据的状态进行更新
        // 传递的参数中含有两个值
        // 第一个值表示相同的key的value数据
        // 第二个值表示缓存区相同key的value数据
        
        val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
            (seq: Seq[Int], opt: Option[Int]) => {
                val newVal: Int = opt.getOrElse(0) + seq.sum
                Option(newVal)
            }
        )
        
        state.print()
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    
}
