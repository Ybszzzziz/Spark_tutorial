package com.atguigu.bigdata.spark.core.test

/**
 * @author Yan
 * @create 2023-08-22 9:56
 * */
class subTask extends Serializable {
  var datas: List[Int] = _
  var logic: (Int) => Int = _

  def compute() = {
    datas.map(logic)
  }
}
