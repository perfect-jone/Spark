package com.atjone.bigdata.spark.operate.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark14_union {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("unionRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val rdd1: RDD[Int] = sc.parallelize(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(5 to 10)

    //union算子:并集
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    rdd3.collect().foreach(println)
  }
}
