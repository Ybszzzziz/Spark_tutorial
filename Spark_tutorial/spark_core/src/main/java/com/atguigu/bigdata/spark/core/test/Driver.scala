package com.atguigu.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

/**
 * @author Yan
 * @create 2023-08-22 9:34
 * */
object Driver {
  def main(args: Array[String]): Unit = {
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)

    val stream1 = client1.getOutputStream
    val objectOutputStream1 = new ObjectOutputStream(stream1)

    val task = new Task()
    val subTask = new subTask()
    subTask.logic = task.logic
    subTask.datas = task.data.take(task.data.length / 2)

    objectOutputStream1.writeObject(subTask)
    objectOutputStream1.flush()
    objectOutputStream1.close()
    client1.close()

    val stream2 = client2.getOutputStream
    val objectOutputStream2 = new ObjectOutputStream(stream2)

    val task1 = new Task()
    val subTask1 = new subTask()
    subTask1.logic = task.logic
    subTask1.datas = task.data.takeRight(task.data.length / 2)

    objectOutputStream2.writeObject(subTask1)
    objectOutputStream2.flush()
    objectOutputStream2.close()
    client2.close()
    println("客户端发数据完毕")
  }
}
