package com.atguigu.bigdata.spark.streaming

import com.atguigu.bigdata.spark.streaming.SparkStreaming011_Req1_BlackList1.AdClickData
import com.atguigu.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming012_Req2 {
    
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
        val clickData: DStream[AdClickData] = kafkaDataDS.map(
            data => {
                val line: String = data.value()
                val splits: Array[String] = line.split(" ")
                AdClickData(splits(0), splits(1), splits(2), splits(3), splits(4))
            }
        )
        
        val reduceDS: DStream[((String, String, String, String), Int)] = clickData.map(
            data => {
                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                val date: String = sdf.format(new Date(data.timeStamp.toLong))
                val day = date
                val area = data.area
                val city = data.city
                val ad = data.ad
                ((day, area, city, ad), 1)
            }
        ).reduceByKey(_ + _)
        
        reduceDS.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    iter => {
                        val conn: Connection = JDBCUtil.getConnection
                        val sql =
                            """
                              |insert into area_city_ad_count(dt, area, city, adid, count)
                              |values(?,?,?,?,?)
                              |on duplicate key
                              |update count = count + ?
                              |""".stripMargin
                        val pstat: PreparedStatement = conn.prepareStatement(sql)
                        iter.foreach{
                            case ((day, area, city, ad), sum) => {
                                pstat.setString(1, day)
                                pstat.setString(2, area)
                                pstat.setString(3, city)
                                pstat.setString(4, ad)
                                pstat.setInt(5, sum)
                                pstat.setInt(6, sum)
                                pstat.executeUpdate()
                            }
                        }
                        pstat.close()
                        conn.close()
                    }
                )
            }
        )
        
        
        
        
        ssc.start()
        ssc.awaitTermination()
    }
    
    // 广告点击数据
    case class AdClickData(timeStamp: String, area: String, city: String, user: String, ad: String)
    
    
    
}
