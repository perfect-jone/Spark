package com.atjone.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//idea中自动生成变量类型设置：Ctal+Alt+S --> Editor --> Code Style --> Scala --> 勾选Local definition
object Spark02_map {
  def main(args: Array[String]): Unit = {
    //创建Spark上下文对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("mapRDD")
    val sc: SparkContext = new SparkContext(conf)


    //从内存中创建RDD
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //map算子
    //所有RDD算子的计算功能全都由Executor执行
    val mapRDD: RDD[Int] = listRDD.map(_ * 2)
    mapRDD.collect().foreach(println)
  }
}
