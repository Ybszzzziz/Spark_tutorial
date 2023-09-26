package com.atguigu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}

/**
 * @author Yan
 * @create 2023-09-25 10:32
 * */
object Spark02_SparkSQL_UDF {
    
    def main(args: Array[String]): Unit = {
        
        val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        import spark.implicits._
        val df: DataFrame = spark.read.json("datas/user.json")
        df.createOrReplaceTempView("user")
        spark.udf.register("ageAvg", new MyAvgUDAF())
        spark.sql("select ageAvg(age) from user").show()
        
        spark.close()
        
        
    }
    
    /**
     * 自定义聚合函数类
     * 继承UDAF
     * 重写方法
     */
    
    class MyAvgUDAF extends UserDefinedAggregateFunction {
        
        
        override def inputSchema: StructType = {
            // 输入数据的结构
            StructType(
                Array(
                    StructField("age", LongType)
                )
            )
        }
        
        // 缓冲区数据的结构
        override def bufferSchema: StructType = {
            StructType(
                Array(
                    StructField("age", LongType),
                    StructField("count", LongType)
                )
            )
        }
        
        // 函数计算结果的数据类型：Out
        override def dataType: DataType = LongType
        
        // 函数的稳定性
        override def deterministic: Boolean = true
        
        // 初始化缓冲区
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 0L
        }
        
        // 根据输入的值更新缓冲区数据
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0) + input.getLong(0))
            buffer.update(1, buffer.getLong(1) + 1)
            
        }
        
        // 缓冲区数据合并
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
            buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
        }
        
        // 计算平均值
        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0) / buffer.getLong(1)
        }
    }
    
}

