package com.atguigu.bigdata.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming013_Req31 {
    
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
        
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("atguiguSpark"), kafkaPara)
        )
        val clickData: DStream[AdClickData] = kafkaDataDS.map(
            data => {
                val line: String = data.value()
                val splits: Array[String] = line.split(" ")
                AdClickData(splits(0), splits(1), splits(2), splits(3), splits(4))
            }
        )
        
        // 设计窗口的计算
        val reduceDS: DStream[(Long, Int)] = clickData.map(
            data => {
                val ts = data.timeStamp.toLong
                val newTs = ts / 10000 * 10000
                (newTs, 1)
            }
        ).reduceByKeyAndWindow((x: Int, y: Int) => {
            x + y
        }, Seconds(60), Seconds(10))
        
        reduceDS.foreachRDD(
            rdd => {
                val list: ListBuffer[String] = ListBuffer[String]()
                val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
                datas.foreach{
                    case (time, cnt) => {
                        val sdf = new SimpleDateFormat("mm:ss")
                        val newTime: String = sdf.format(new Date(time))
                        list.append(s"""{"xtime":"${newTime}", "yval":"${cnt}"}""".stripMargin)
                    }
                }
                
                val out = new PrintWriter(new FileWriter(new File("D:\\Unqyan\\spark\\Spark_tutorial\\datas\\adclick\\adclick.json")))
                out.println("[" + list.mkString(",") + "]")
                out.flush()
                out.close()
            }
        )
        
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    // 广告点击数据
    case class AdClickData(timeStamp: String, area: String, city: String, user: String, ad: String)
    
    
    
}
