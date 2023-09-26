package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}

/**
 * @author Yan
 * @create 2023-09-25 10:32
 * */
object Spark03_SparkSQL_UDF {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        val df: DataFrame = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")
        spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF))
        spark.sql("select ageAvg(age) from user").show()
        
        spark.close()
        
        
    }
    
    /**
     * 自定义聚合函数类
     * 继承Aggregator 定义泛型
     * IN:输入数据类型 Long
     * OUT:输出数据类型 Long
     * M:缓冲区数据类型
     * 重写方法
     */
    
    case class Buff(var total: Long, var count: Long)
    
    class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
        
        // 初始值
        override def zero: Buff = {
            Buff(0L, 0L)
        }
        
        // 根据输入数据更新
        override def reduce(b: Buff, a: Long): Buff = {
            b.total  = b.total + a
            b.count += 1
            b
        }
        
        override def merge(b1: Buff, b2: Buff): Buff = {
            b1.total  = b1.total + b2.total
            b1.count += b2.count
            b1
        }
        
        override def finish(reduction: Buff): Long = {
            reduction.total / reduction.count
        }
        
        // 缓冲区编码操作
        override def bufferEncoder: Encoder[Buff] = Encoders.product
        
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
    
}

