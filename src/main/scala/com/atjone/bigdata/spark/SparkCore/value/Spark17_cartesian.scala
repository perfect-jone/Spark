package com.atjone.bigdata.spark.SparkCore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_cartesian {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cartesianRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val rdd1: RDD[Int] = sc.parallelize(1 to 3)
    val rdd2: RDD[Int] = sc.parallelize(3 to 5)

    //cartesian算子：笛卡尔积
    val rdd3: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    rdd3.collect().foreach(println)
  }
}
