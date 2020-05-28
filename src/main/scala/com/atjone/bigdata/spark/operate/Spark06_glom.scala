package com.atjone.bigdata.spark.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_glom {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("glomRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象,设置5个分区
    val listRDD: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5, 6, 7, 8), 3)

    //glom算子:将一个分区的数据放到一个数组中
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
    glomRDD.collect().foreach(arr =>println(arr.mkString(",")))
  }
}
