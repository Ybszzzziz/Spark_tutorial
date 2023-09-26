package com.atguigu.bigdata.spark.core.test

/**
 * @author Yan
 * @create 2023-08-22 9:46
 * */
class Task extends Serializable {

  val data = List(1, 2, 3, 4)

//  val logic = ( num: Int ) => { num * 2}
  val logic: (Int) => Int = _ * 2

}
