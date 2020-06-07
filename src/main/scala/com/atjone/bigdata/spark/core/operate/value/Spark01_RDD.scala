package com.atjone.bigdata.spark.operate.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {

    //创建Spark上下文对象
    val conf: SparkConf = new SparkConf().setAppName("RDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //创建RDD
    //1.从内存中创建 makeRDD,底层实现是parallelize
    //conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10),10)

    //2.从内存中创建 parallelize
    val arrayRDD: RDD[String] = sc.parallelize(Array("1","2","3","4"))

    //3.从外部存储中创建
    //默认情况下读取项目路径，也可以读取其他路径：HDFS
    //默认从文件中读取的数据都是字符串类型
    //def defaultMinPartitions: Int = math.min(defaultParallelism, 2)
    //取文件时，传递的分区参数为最小分区数，但是不一定是这个分区数，取决于hadoop读取文件时的分片规则
    val fileRDD: RDD[String] = sc.textFile("input")

    //listRDD.collect().foreach(println)

    //将RDD的数据保存到文件中
    //fileRDD.saveAsTextFile("output")
    //listRDD.saveAsTextFile("output")
    listRDD.saveAsTextFile("output")
  }
}
