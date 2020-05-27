package com.atjone.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    //设定Spark计算框架的运行（部署）环境
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建Spark上下文对象
    val sc: SparkContext = new SparkContext(config)
    val res = sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).collect()
    res.foreach(println)
  }
}
