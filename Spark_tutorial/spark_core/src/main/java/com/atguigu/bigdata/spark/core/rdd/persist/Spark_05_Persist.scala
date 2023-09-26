package com.atguigu.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 15:01
 * */
object Spark_05_Persist {
    def main(args: Array[String]): Unit = {
        
        // cache : 将数据临时存储在内存中进行数据重用
        //          会在血缘关系中添加新的依赖。一旦出现问题 可以重头读取
        // persist ：将数据临时存储在磁盘文件中进行数据重用
        //           涉及到磁盘IO 性能低 但是安全
        //           job执行完毕后 临时文件就会删除
        // checkpoint：长久保存在磁盘当中，进行磁盘重用
        //           涉及到磁盘IO 性能低 但是安全
        //           为了保证数据安全 一般情况下会独立作业
        //           为了提高效率 一般情况下，是需要和cache联合使用
        //           执行过程中会切断血缘关系，重新建立新的血缘关系
        //           checkpoint 等同于该边数据源
        
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")
        val sc = new SparkContext(sparkConf)
        
        val list = List("hello scala", "hello saprk")
        val rdd: RDD[String] = sc.makeRDD(list)
        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = flatRDD.map(
            word => {
//                println("********")
                (word, 1)
        }
        )
        sc.setCheckpointDir("cp")
        // checkPoint需要落盘
        // 作业执行完毕后不会被删除
        // 一般保存在hdfs中
//        mapRDD.cache()
        mapRDD.checkpoint()
        println(mapRDD.toDebugString)
        val resRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
        resRDD.collect().foreach(println)
        println("******************")
        
        println(mapRDD.toDebugString)
        
        
        
        
        sc.stop()
    }
    
}
