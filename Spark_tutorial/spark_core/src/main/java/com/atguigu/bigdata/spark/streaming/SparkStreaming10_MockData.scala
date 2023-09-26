    package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random
    
    /**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming10_MockData {
        
        
        def main(args: Array[String]): Unit = {
        
        // 生成模拟数据
        // timestamp area  city userid  adid
        // 时间戳     区域  城市  用户     广告
        
        // Application => Kafka => SparkStreaming => Analysis
        
        // 创建配置对象
        val prop = new Properties()
        // 添加配置
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringSerializer")
        
        val producer = new KafkaProducer[String, String](prop)
        
        while (true) {
            mockData().foreach(
                data => {
                    // 向Kafka生成数据
                    
                    val record: ProducerRecord[String, String] =
                        new ProducerRecord[String, String]("atguiguSpark", data)
                    producer.send(record)
                }
            )
            Thread.sleep(2000)
        }
       
    }
        
        def mockData(): ListBuffer[String] = {
            val list: ListBuffer[String] = ListBuffer[String]()
            val areaList: ListBuffer[String] = ListBuffer[String]("华北", "华东", "华南")
            val cityList: ListBuffer[String] = ListBuffer[String]("北京", "上海", "深圳")
            for (i <- 1 to new Random().nextInt(60)) {
                val area = areaList(new Random().nextInt(3))
                val city = cityList(new Random().nextInt(3))
                var userId = new Random().nextInt(6)
                var adId = new Random().nextInt(6)
                list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adId}")
            }
            list
        }
    
    
}
