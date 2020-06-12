package com.atjone.bigdata.spark.SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 无状态转换:采集器每个周期采集一次数据每个周期间是独立的；有状态转换：保留上个周期数据和当前周期数据进行更新
object SparkStreaming05_UpdateState {
  def main(args: Array[String]): Unit = {

    // Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    // 实时数据分析环境对象：配置信息，采集周期
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //设置有状态转换检查点保存目录
    streamingContext.sparkContext.setCheckpointDir("out")

    // 从Kafka中采集数据,ReceiverInputDStream[(String, String)],第一个String是Key,第二个String是Vavalu
    val kafkaDStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      streamingContext,
      "hadoop101:2181,hadoop102:2181,hadoop103:2181",
      "atjone",
      Map("first" -> 3),
      StorageLevel.MEMORY_ONLY
    )

    // 将采集的数据进行扁平化
    val wordDStream: DStream[String] = kafkaDStream.flatMap(_._2.split(" "))

    // 将数据进行结构转换
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    // 将转换后的数据按照相同的key进行聚合
    //val wordCountDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_ + _)

    //有状态转换
    val statDStream: DStream[(String, Int)] = mapDStream.updateStateByKey {
      case (seq, buffer) => {
        val sum = buffer.getOrElse(0) + seq.sum
        // (Hello ,1)
        Option(sum)
      }
    }

    // 将结果打印出来
    statDStream.print()

    // 不能停止采集程序

    // 启动采集器
    streamingContext.start()

    // Driver等待采集器的执行，采集器停，Driver才可以停
    streamingContext.awaitTermination()
  }
}

