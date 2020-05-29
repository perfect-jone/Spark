package com.atjone.bigdata.spark.operate.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_zip {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("zipRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val rdd1: RDD[Int] = sc.parallelize(1 to 3, 3)
    val rdd2: RDD[Int] = sc.parallelize(3 to 5, 3)

    //zip(拉链)算子：将两个RDD组合成Key/Value形式的RDD,这里默认两个RDD的partition数量以及元素数量都相同，否则会抛出异常
    val rdd3: RDD[(Int, Int)] = rdd1.zip(rdd2)
    rdd3.collect().foreach(println)
  }
}
