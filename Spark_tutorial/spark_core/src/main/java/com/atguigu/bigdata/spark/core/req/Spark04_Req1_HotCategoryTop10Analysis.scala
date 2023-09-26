package com.atguigu.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Yan
 * @create 2023-09-20 8:57
 * */
object Spark04_Req1_HotCategoryTop10Analysis {
    
    def main(args: Array[String]): Unit = {
    
        // TODO: top10
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
                .setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparkConf)
        
        // TODO: optimize
        // 存在大量的shuffle （reduceByKey）
        // reduceByKey聚合算子，spark会优化 缓存
        
        // 1.读取原始日志数据
        val rdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
        
        val acc = new HotCategoryAccumulator()
        sc.register(acc, "hotCategory")
        
        // 2.数据转换结构
        //   点击的场合：（品类ID， （1，0，0））
        //   下单的场合：（品类ID， （0，1，0））
        //   支付的场合：（品类ID， （0，0，1））
        rdd.foreach {
            action => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击
                    acc.add((datas(6), "click"))
                } else if (datas(8) != "null") {
                    // 下单
                    val ids: Array[String] = datas(8).split(",")
                    ids.foreach(acc.add(_, "order"))
                } else if (datas(10) != "null") {
                    // 支付
                    val ids: Array[String] = datas(10).split(",")
                    ids.foreach(acc.add(_, "pay"))
                } else {
                    Nil
                }
            }
        }
        
        val accVal: mutable.Iterable[HotCategory] = acc.value.map(_._2)
        val res: List[HotCategory] = accVal.toList.sortWith(
            (left, right) => {
                if (left.clickCnt > right.clickCnt) {
                    true
                } else if (left.clickCnt == right.clickCnt) {
                    if (left.orderCnt > right.orderCnt) {
                        true
                    } else if (left.orderCnt == right.orderCnt) {
                        left.payCnt > right.payCnt
                    }
                    else {
                        false
                    }
                }
                else {
                    false
                }
            }
        )
        res.take(10).foreach(println)
        
        sc.stop()
    }
    
    case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)
    
    /**
     * IN：外部传到累加器的值，（品类ID， 行为类型）
     * OUT：mutable.Map[String, HotCategory]
     */
    class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {
        
        private val hcMap = mutable.Map[String, HotCategory]()
        
        override def isZero: Boolean = hcMap.isEmpty
        
        override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = new HotCategoryAccumulator()
        
        override def reset(): Unit = hcMap.clear()
        
        override def add(v: (String, String)): Unit = {
            val cid: String = v._1
            val actionType: String = v._2
            val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
            if (actionType == "click") {
                category.clickCnt += 1
            } else if (actionType == "order") {
                category.orderCnt += 1
            } else if (actionType == "pay") {
                category.payCnt += 1
            }
            hcMap.update(cid, category)
        
        }
        
        override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
            
            val map1 = this.hcMap
            val map2 = other.value
            map2.foreach{
                case (cid, hc) => {
                    val curHc: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
                    curHc.clickCnt += hc.clickCnt
                    curHc.orderCnt += hc.orderCnt
                    curHc.payCnt += hc.payCnt
                    map1.update(cid, curHc)
                }
            }
            
        }
        
        override def value: mutable.Map[String, HotCategory] = hcMap
    }
    
}
