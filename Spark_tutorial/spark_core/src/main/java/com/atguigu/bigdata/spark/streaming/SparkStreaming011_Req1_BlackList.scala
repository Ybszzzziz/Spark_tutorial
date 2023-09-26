package com.atguigu.bigdata.spark.streaming

import com.atguigu.bigdata.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author Yan
 * @create 2023-09-25 19:39
 * */
object SparkStreaming011_Req1_BlackList {
    
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
        
        
        val dstream: DStream[((String, String, String), Int)] = clickData.transform(
            rdd => {
                // 周期性获取黑名单数据
                val blackList = ListBuffer[String]()
                val conn: Connection = JDBCUtil.getConnection
                val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list")
                
                val rs: ResultSet = pstat.executeQuery()
                while (rs.next()) {
                    blackList.append(rs.getString(1))
                }
                rs.close()
                pstat.close()
                conn.close()
                
                // 判断点击用户是否在黑名单中
                val filterRDD: RDD[AdClickData] = rdd.filter(
                    data => {
                        !blackList.contains(data)
                    }
                )
                
                // 如果用户不在黑名单中，那么进行统计数量（每个采集周期）
                filterRDD.map(
                    data => {
                        val sdf = new SimpleDateFormat("yyyy-MM-dd")
                        val day: String = sdf.format(new Date(data.timeStamp.toLong))
                        val user: String = data.user
                        val ad: String = data.ad
                        
                        ((day, user, ad), 1)
                    }
                ).reduceByKey(_ + _)
                
            }
        )
        dstream.foreachRDD(
            rdd => {
                rdd.foreach{
                    case ((day, user, ad), cnt) => {
                        if (cnt >= 30) {
                            // 如果统计数量超过阈值，将点击数量拉入黑名单
                            val conn: Connection = JDBCUtil.getConnection
                            val pstat1: PreparedStatement = conn.prepareStatement(
                                """
                                  |insert into black_list(userid) values(?)
                                  |on duplicate key
                                  |update userid = ?
                                  |""".stripMargin)
                            pstat1.setString(1, user)
                            pstat1.setString(2, user)
                            pstat1.executeUpdate()
                            pstat1.close()
                            conn.close()
                        } else {
                            // 如果没有超过阈值，需要将当天的广告点击数量更新
                            val conn: Connection = JDBCUtil.getConnection
                            val pstat2: PreparedStatement = conn.prepareStatement("select * from user_ad_count" +
                                    " where dt = ? and userid = ? and adid = ?")
                            pstat2.setString(1, day)
                            pstat2.setString(2, user)
                            pstat2.setString(3, ad)
                            val rs: ResultSet = pstat2.executeQuery()
                            if (rs.next()) {
                                val pstat3 = conn.prepareStatement(
                                    """
                                      |update user_ad_count
                                      |set count = count + ?
                                      |where dt = ? and userid = ? and adid = ?
                                      |""".stripMargin)
                                pstat3.setInt(1, cnt)
                                pstat3.setString(2, day)
                                pstat3.setString(3, user)
                                pstat3.setString(4, ad)
                                pstat3.executeUpdate()
                                pstat3.close()
                                
                                // 判断更新的数据是否超过阈值，如果超过拉入黑名单
                                val pstat4: PreparedStatement = conn.prepareStatement("select * from user_ad_count" +
                                        " where dt = ? and userid = ? and adid = ? and count >= 30")
                                pstat4.setString(1, day)
                                pstat4.setString(2, user)
                                pstat4.setString(3, ad)
                                val rs4: ResultSet = pstat4.executeQuery()
                                if (rs4.next()) {
                                    val conn: Connection = JDBCUtil.getConnection
                                    val pstat5: PreparedStatement = conn.prepareStatement(
                                        """
                                          |insert into black_list(userid) values(?)
                                          |on duplicate key
                                          |update userid = ?
                                          |""".stripMargin)
                                    pstat5.setString(1, user)
                                    pstat5.setString(2, user)
                                    pstat5.executeUpdate()
                                    pstat5.close()
                                    conn.close()
                                }
                                rs4.close()
                                pstat4.close()
                            } else {
                                val pstat6: PreparedStatement = conn.prepareStatement(
                                    """
                                      |insert into user_ad_count(dt, userid, adid, count)
                                      |values(?,?,?,?)
                                      |""".stripMargin)
                                pstat6.setString(1, day)
                                pstat6.setString(2, user)
                                pstat6.setString(3, ad)
                                pstat6.setInt(4, cnt)
                                pstat6.executeUpdate()
                                pstat6.close()
                            }
                            rs.close()
                            pstat2.close()
                            conn.close()
                            
                            
                        }
                    }
                }
            }
        )
        ssc.start()
        ssc.awaitTermination()
    }
    
    // 广告点击数据
    case class AdClickData(timeStamp: String, area: String, city: String, user: String, ad: String)
    
    
    
}
