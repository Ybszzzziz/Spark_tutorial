package com.atguigu.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Yan
 * @create 2023-08-23 10:10
 * */
object Spark17_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)),
      2)
//    rdd.aggregateByKey(5)(_+_, _+_).collect().foreach(println)

    // 如果聚合计算时  分区内和分区间计算规则相同，spark提供了更简单的方法
//    rdd.foldByKey(0)(_+_).collect().foreach(println)
    // 求平均值
    // combineByKey
    // 第一个参数表示：将相同key的第一个数据进行结构的转换 ，实现操作
    // 第二个参数表示：分区内的计算规则
    // 第三个参数表示：分区间的计算规则
    val aggRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v, 1),
      (t:(Int, Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1:(Int,Int), t2:(Int,Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )
    rdd.combineByKey(v=>v, (x:Int, y)=>x+y, (x:Int,y:Int)=>x+y).collect().foreach(println )
//    val rddTemp: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    rddTemp.map(_*2)
//
//    val result: RDD[(String, Int)] = aggRDD.map {
//      case (key, (a, b)) => {
//        (key, a / b)
//      }
//    }
//    val result: RDD[(String, Int)] = aggRDD.mapValues {
//      case (a, b) => {
//        a / b
//      }
//    }
    aggRDD.collect().foreach(println)
    sc.stop()



  }

}
