package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jsonwritable

import scala.collection.mutable

/**
 * @author Yan
 * @create 2023-08-21 11:04
 * */
object Spark04_WordCount {
  def main(args: Array[String]): Unit = {
    
    // application
    // spark框架
    // TODO建立和spark的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    wordcount11(sc: SparkContext)
    
    sc.stop()
  }
  
  def wordcount1(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = splits.groupBy(split => split)
    val res: RDD[(String, Int)] = group.mapValues(_.size)
    
  }
  
  def wordcount2(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRes: RDD[(String, Int)] = splits.map((_, 1))
    val res: RDD[(String, Iterable[Int])] = mapRes.groupByKey()
    val ans: RDD[(String, Int)] = res.mapValues(_.size)
    
  }
  
  def wordcount3(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRes: RDD[(String, Int)] = splits.map((_, 1))
    val res: RDD[(String, Int)] = mapRes.reduceByKey(_+_)
  }
  
  def wordcount4(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRes: RDD[(String, Int)] = splits.map((_, 1))
    val res: RDD[(String, Int)] = mapRes.aggregateByKey(0)(_ + _, _+_)
  }
  
  def wordcount5(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRes: RDD[(String, Int)] = splits.map((_, 1))
    val res: RDD[(String, Int)] = mapRes.foldByKey(0)(_+_)
  }
  
  def wordcount6(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRes: RDD[(String, Int)] = splits.map((_, 1))
    val res: RDD[(String, Int)] = mapRes.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
  }
  
  def wordcount7(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRes: RDD[(String, Int)] = splits.map((_, 1))
    val stringToLong: collection.Map[String, Long] = mapRes.countByKey()
    
  }
  
  def wordcount8(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val res: collection.Map[String, Long] = splits.countByValue()
    
  }
  
  def wordcount9(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[mutable.Map[String, Long]] = splits.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val ans: mutable.Map[String, Long] = mapRdd.reduce(
      (map1, map2) => {
        map2.foreach {
          case (k, v) => {
            val res: Long = map1.getOrElse(k, 0L) + v
            map1.update(k, res);
          }
        }
        map1
      }
    )
    println(ans)
    sc.stop()
    
  }
  
  def wordcount10(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    println(splits.aggregate(mutable.Map[String, Long]())(
      (mp, word) => {
        val res: Long = mp.getOrElse(word, 0L) + 1
        mp.update(word, res)
        mp
      },
      (map1, map2) => {
        map2.foreach {
          case (k, v) => {
            val newV: Long = map1.getOrElse(k, 0L) + v
            map1.update(k, newV)
          }
        }
        map1
      }
    ))
    
  }
  
  def wordcount11(sc: SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(List("Hello Scala", "Hello World"))
    val splits: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[mutable.Map[String, Long]] = splits.map {
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    }
    println(mapRDD.fold(mutable.Map[String, Long]())(
      (map1, map2) => {
        map2.foreach {
          case (k, v) => {
            val newValue: Long = map1.getOrElse(k, 0L) + v
            map1.update(k, newValue)
          }
        }
        map1
      }
    ))
  }
  
  
  
  
}
