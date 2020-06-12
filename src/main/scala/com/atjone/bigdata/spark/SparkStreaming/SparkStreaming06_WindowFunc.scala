package com.atjone.bigdata.spark.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 窗口函数应用：天气温度实时监控、园区的人流量实时监控、高速车流量实时监控
object SparkStreaming06_WindowFunc {
  def main(args: Array[String]): Unit = {

    // Scala的sliding函数，类似于Spark的window函数
    val ints = List(1, 2, 3, 4, 5, 6)
    val intses: Iterator[List[Int]] = ints.sliding(3, 1)
    for (elem <- intses) {
      println(elem.mkString(","))
    }

    // Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    // 实时数据分析环境对象：配置信息，采集周期
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 从Kafka中采集数据,ReceiverInputDStream[(String, String)],第一个String是Key,第二个String是Vavalu
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop101:2181,hadoop102:2181,hadoop103:2181",
      "atjone",
      Map("first" -> 3),
      StorageLevel.MEMORY_ONLY
    )

    //窗口函数：第一个参数表示窗口大小，第二个参数表示窗口滑动步长，两个参数都是采集周期的整数倍
    val windowDStream: DStream[(String, String)] = kafkaDStream.window(Seconds(9),Seconds(3))

    // 将采集的数据进行扁平化
    val wordDStream: DStream[String] = windowDStream.flatMap(_._2.split(" "))

    // 将数据进行结构转换
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换后的数据按照相同的key进行聚合
    val wordCountDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    // 将结果打印出来
    wordCountDStream.print()

    // 不能停止采集程序

    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行，采集器停，Driver才可以停
    streamingContext.awaitTermination()
  }
}

