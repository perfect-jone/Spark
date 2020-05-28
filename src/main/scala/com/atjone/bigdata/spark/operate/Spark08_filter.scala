package com.atjone.bigdata.spark.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_filter {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("filterRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //filter算子:生成数据，按照指定的规则进行过滤
    val filterRDD: RDD[Int] = listRDD.filter(x => x % 2 == 0)
    filterRDD.collect().foreach(println)
  }
}
