package com.atjone.bigdata.spark.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_coalesce {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("coalesceRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.parallelize(1 to 16, 4)

    // coalesce算子:缩减分区，可以简单理解为合并分区，数据并没有打乱重组，所以不是shuffle操作。

    val coalesceRDD: RDD[Int] = listRDD.coalesce(2)
    //coalesceRDD.collect().foreach(println)
    coalesceRDD.saveAsTextFile("output")
  }
}
