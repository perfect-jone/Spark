package com.atjone.bigdata.spark.operate.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark19_partitionBy {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f")))

    //partitionBy算子(k-v)：对pairRDD进行分区操作，如果原有的partionRDD和现有的partionRDD是一致的话就不进行分区，
    //否则会生成ShuffleRDD，即会产生shuffle过程
    val rdd: RDD[(Int, String)] = listRDD.partitionBy(new MyPartitioner(2))
    rdd.saveAsTextFile("output")
  }
}

//自定义分区器
class MyPartitioner(partitions: Int) extends Partitioner {

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(key.hashCode, numPartitions)
  }
}
