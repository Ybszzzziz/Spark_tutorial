package com.atguigu.bigdata.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-09-19 8:30
 * */
object Spark01_RDD_Operator_Action {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("Local[*]").setAppName("Operator")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 出发作业执行的方法 job
        // 底层调用环境对象的runJob方法 ， 创建ActiveJob 提交执行
        rdd.collect()

        sc.stop()
    }
}
