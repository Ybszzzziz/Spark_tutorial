package com.atguigu.bigdata.spark.sql

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author Yan
 * @create 2023-09-25 10:32
 * */
object Spark06_SparkSQL_HIVE_case1 {
    
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME","atguigu")
        val spark: SparkSession = SparkSession.builder().config("hive.metastore.uris", "thrift://hadoop102:9083").enableHiveSupport().master("local[*]").
                appName("spark-sql").getOrCreate()
        // 使用sparksql 连接外置hive
        // 1.拷贝Hive-site.xml 到classpath
        // 2.启动Hive的支持
        // 3.增加对应的依赖关系（包含mysql的驱动）
        
        spark.udf.register("cityRemark", functions.udaf(new MyUDAF))
        
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    clickCnt,
              |    city_remark
              |from (
              |      select
              |          area,
              |          product_name,
              |          clickCnt,
              |          city_remark,
              |          rank() over (partition by area order by clickCnt desc) rk
              |      from (
              |            select
              |                area,
              |                product_name,
              |                count(*) clickCnt,
              |                cityRemark(city_name) city_remark
              |            from (
              |                  select
              |                      uva.*,
              |                      pi.product_name,
              |                      ci.area,
              |                      ci.city_name
              |                  from user_visit_action uva
              |                  left join product_info pi on uva.click_product_id = pi.product_id
              |                  left join city_info ci on uva.city_id = ci.city_id
              |                  where click_product_id != -1) t1
              |            group by area, product_name) t2) t3
              |where rk <= 3;
              |""".stripMargin).show()
        
        spark.close()
        
        
    }
    
    case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])
    
    /**
     *
     */
    class MyUDAF extends Aggregator[String, Buffer, String] {
        
        override def zero: Buffer = Buffer(0L, mutable.Map[String, Long]())
        
        // 更新缓冲区数据
        override def reduce(buffer1: Buffer, city: String): Buffer = {
            buffer1.total += 1L
            val newVal: Long = buffer1.cityMap.getOrElse(city, 0L) + 1L
            buffer1.cityMap.update(city, newVal)
            buffer1
        }
        // 合并缓冲区
        override def merge(b1: Buffer, b2: Buffer): Buffer = {
            b1.total += b2.total
            val map1 = b1.cityMap
            val map2 = b2.cityMap
            map2.foreach {
                case (city, cnt) => {
                    val newVal: Long = map1.getOrElse(city, 0L) + cnt
                    map1.update(city, newVal)
                    map1
                }
            }
            b1.cityMap = map1
            b1
        }
        
        // 将统计的结果生成字符串
        override def finish(buffer: Buffer): String = {
            val remarkList = ListBuffer[String]()
            val totalCnt = buffer.total
            val map = buffer.cityMap
            val hasMore = map.size > 2
            val cntList: List[(String, Long)] = map.toList.sortWith(
                (l, r) => {
                    l._2 > r._2
                }
            ).take(2)
            var rsum = 0L
            cntList.foreach{
                case (city, cnt) => {
                    val r = cnt * 100 / totalCnt
                    remarkList.append(s"${city} ${r}%")
                    rsum += r
                }
            }
            if (hasMore) {
                remarkList.append(s"其他 ${100 - rsum}%")
            }
            remarkList.mkString(",")
        }
        
        override def bufferEncoder: Encoder[Buffer] = Encoders.product
        
        override def outputEncoder: Encoder[String] = Encoders.STRING
    }
    
}

