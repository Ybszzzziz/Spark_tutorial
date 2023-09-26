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
object SparkStreaming011_Req1_BlackList1 {
    
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
        
        /**
         * foreach方法是RDD的算子，算子之外的代码实在Driver端执行，算子内的代码是在Executor端执行
         * 这样就会设计闭包操作，Driver端的数据就需要传递到Executor端，需要将数据进行序列化
         * 数据库的连接对象是不能序列化的
         *
         * RDD提供了一个算子可以有效提升效率：foreachPartition
         * 可以一个分区创建一个连接对象，可以大幅度减少连接对象的数量，提升效率
         */
        
        
        
        dstream.foreachRDD(
            rdd => {
                
                rdd.foreachPartition(
                    iter => {
                        val conn: Connection = JDBCUtil.getConnection
                        iter.foreach{
                            case ((day, user, ad), cnt) => {
                                if (cnt >= 30) {
                                    // 如果统计数量超过阈值，将点击数量拉入黑名单
                                    val sql =
                                        """
                                          |insert into black_list(userid) values(?)
                                          |on duplicate key
                                          |update userid = ?
                                          |""".stripMargin
                                    JDBCUtil.executeUpdate(conn, sql, Array(user, user))
                                    conn.close()
                                } else {
                                    // 如果没有超过阈值，需要将当天的广告点击数量更新
                                    val conn: Connection = JDBCUtil.getConnection
                                    val sql = "select * from user_ad_count" +
                                            " where dt = ? and userid = ? and adid = ?"
                                    val flag: Boolean = JDBCUtil.isExist(conn, sql, Array(day, user, ad))
                                    if (flag) {
                                        val sql1 =
                                            """
                                              |update user_ad_count
                                              |set count = count + ?
                                              |where dt = ? and userid = ? and adid = ?
                                              |""".stripMargin
                                        JDBCUtil.executeUpdate(conn, sql1, Array(cnt, day, user, ad))
                                        
                                        // 判断更新的数据是否超过阈值，如果超过拉入黑名单
                                        val sql2 = "select * from user_ad_count" +
                                                " where dt = ? and userid = ? and adid = ? and count >= 30"
                                        val flag2 = JDBCUtil.isExist(conn, sql2, Array(day, user, ad))
                                        if (flag2) {
                                            val conn: Connection = JDBCUtil.getConnection
                                            val sql3 =
                                                """
                                                  |insert into black_list(userid) values(?)
                                                  |on duplicate key
                                                  |update userid = ?
                                                  |""".stripMargin
                                            JDBCUtil.executeUpdate(conn, sql3, Array(user, user))
                                            conn.close()
                                        }
                                    } else {
                                        val sql4 =
                                            """
                                              |insert into user_ad_count(dt, userid, adid, count)
                                              |values(?,?,?,?)
                                              |""".stripMargin
                                        JDBCUtil.executeUpdate(conn, sql4, Array(day, user, ad, cnt))
                                    }
                                    conn.close()
                                }
                            }
                        }
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
