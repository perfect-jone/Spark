package com.atjone.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_mapPartitions {
  def main(args: Array[String]): Unit = {

    //创建Spark上下文对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionsRDD")
    val sc: SparkContext = new SparkContext(conf)


    //从内存中创建RDD
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //mapPartitions算子
    //等同于这个写法：listRDD.mapPartitions(datas =>{datas.map(data => data * 2)})
    //mapPartitions可以对所有的分区进行遍历，效率优于map算子，但可能会造成内存溢出（OOM）
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(_.map(_ * 2))
    mapPartitionsRDD.collect().foreach(println)
  }
}
