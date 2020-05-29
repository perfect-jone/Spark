package com.atjone.bigdata.spark.operate.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_combineByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)

    /**
      * createCombiner: V => C,
      * mergeValue: (C, V) => C,
      * mergeCombiners: (C, C) => C
      */
    val combineRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(v => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val avgRDD: RDD[(String, Double)] = combineRDD.map(x => (x._1, x._2._1 / x._2._2.toDouble))
    avgRDD.collect().foreach(println)

  }
}


