package com.atjone.bigdata.spark.SparkStreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreaming03_MyReceive {
  def main(args: Array[String]): Unit = {

    // Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    // 实时数据分析环境对象：配置信息，采集周期
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // 从指定的文件夹中采集数据
    val fileDStream: DStream[String] = streamingContext.receiverStream(new MyReceive("hadoop101",9999))

    // 将采集的数据进行扁平化
    val wordDStream: DStream[String] = fileDStream.flatMap(_.split(" "))

    // 将数据进行结构转换
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换后的数据按照相同的key进行聚合
    val wordCountDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 将结果打印出来
    wordCountDStream.print()

    // 不能停止采集程序

    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}

// 声明采集器
class MyReceive(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var socket: Socket = null

  def receive(): Unit = {
    socket = new Socket(host, port)
    val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, "UTF-8"))
    var line: String = null
    while ((line = reader.readLine()) != null) {

      // 将采集的数据存储到采集器内部进行转换
      if ("end".equals(line)) {
        return
      } else {
        this.store(line)
      }
    }
  }

  override def onStart(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        receive()
      }
    }).start()

  }

  override def onStop(): Unit = {
    if (socket != null) {
      socket.close()
      socket = null
    }
  }
}
