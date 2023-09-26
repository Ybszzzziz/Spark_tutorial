package com.atguigu.bigdata.spark.core.frameWork.service

import com.atguigu.bigdata.spark.core.frameWork.common.TService
import com.atguigu.bigdata.spark.core.frameWork.dao.WordCountDao

/**
 * @author Yan
 * @create 2023-09-20 20:20
 * 服务层
 * */
class WordCountService extends TService{

    private val wordCountDao = new WordCountDao
    
    def dataAnalyze(): Array[(String, Int)] = {
        val lines = wordCountDao.readFile("datas/1.txt")
        val words = lines.flatMap(x => x.split(" "))
        val wordToOne = words.map(word => (word, 1))
        val value = wordToOne.groupBy(x => x._1)
        val value1 = value.map {
            case (word, list) => {
                list.reduce(
                    (t1, t2) => (t1._1, t1._2 + t2._2)
                )
            }
        }
        value1.collect()
    }
    
}
