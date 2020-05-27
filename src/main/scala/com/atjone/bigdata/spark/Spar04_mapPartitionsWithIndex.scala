package com.atjone.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spar04_mapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapPartitionsWithIndexRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象,设置5个分区
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 5)


    //mapPartitionsWithIndex算子
    val mapPartitionsWithIndexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => datas.map((_, "分区号：" + num))
    }
    mapPartitionsWithIndexRDD.collect().foreach(println)
  }
}
