package com.atguigu.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

/**
 * @author Yan
 * @create 2023-08-22 9:41
 * */
object Executor2 {
  def main(args: Array[String]): Unit = {

    val server = new ServerSocket(8888)
    println("服务器启动，接受数据中")
    val client = server.accept()
    val stream = client.getInputStream
    val inputStream = new ObjectInputStream(stream)
    val task = inputStream.readObject().asInstanceOf[subTask]
    val list = task.compute()
    println("[8888]接收到数据" + list)
    inputStream.close()
    client.close()
    server.close()

  }

}
