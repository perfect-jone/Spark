package com.atjone.bigdata.spark.operate.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_sortBy {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sortByRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.parallelize(List(2, 1, 3, 4))

    // sortBy算子:排序，默认升序,ascending=false表示降序
    val sortByRDD: RDD[Int] = listRDD.sortBy(x => x, ascending = false)
    sortByRDD.collect.foreach(println)
  }
}
