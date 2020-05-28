package com.atjone.bigdata.spark.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_distinct {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("distinctRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))

    // distinct算子:对源RDD进行去重后返回一个新的RDD
    //将RDD中一个分区的数据打乱重组到其他不同的分区的操作，称之为shuffle操作
    //Spark中所有的转换算子中没有shuffle的算子，性能比较快
    //val distinctRDD: RDD[Int] = listRDD.distinct()
    //指定两个分区存储去重后的数据
    val distinctRDD: RDD[Int] = listRDD.distinct(2)
    distinctRDD.collect().foreach(println)
  }
}
