package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming011_Req1 {
    
    def main(args: Array[String]): Unit = {
    
        // TODO 创建环境对象
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming01_WordCount")
        
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG ->
                    "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" ->
                    "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" ->
                    "org.apache.kafka.common.serialization.StringDeserializer"
        )
        
        val ssc = new StreamingContext(sparkConf, Seconds(3))
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("atguiguSpark"), kafkaPara)
        )
        kafkaDataDS.map(_.value()).print()
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    
    
}
