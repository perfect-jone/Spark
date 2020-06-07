package com.atjone.bigdata.spark.operate.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark25_sortByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.parallelize(Array(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    val rdd2: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _) //求和
    val rdd3: RDD[(String, Int)] = rdd.foldByKey(0)(math.max(_, _)) //求最大值
    val sumRDD: RDD[(String, (Int, Int))] = rdd3.combineByKey(
      (_, 1),
      (comb: (Int, Int), v) => (comb._1 + v, comb._2 + 1),
      (comb1: (Int, Int), comb2: (Int, Int)) => (comb1._1 + comb2._1, comb1._2 + comb2._2))

    rdd2.collect().foreach(println)


  }
}


