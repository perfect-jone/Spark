package com.atjone.bigdata.spark.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_flatMap {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("flatMapRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    //flatMap算子：扁平化
    // val flatMapRDD: RDD[Int] = listRDD.flatMap(_.map(_ * 2))
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas => datas.map(_ * 2))
    flatMapRDD.collect().foreach(println)
  }
}
