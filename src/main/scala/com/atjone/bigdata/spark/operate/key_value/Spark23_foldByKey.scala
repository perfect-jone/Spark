package com.atjone.bigdata.spark.operate.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_foldByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(Int, Int)] = sc.makeRDD(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    /**
      * foldByKey(zeroValue: V)(func: (V, V) => V): RDD[(K, V)]
      * aggregateByKey中的seqOp是分区内函数，combOp是分区间函数
      * foldByKey是aggregateByKey的一种简化，分区内函数和分区间函数一样，所以可以省略一个函数
      */

    //foldByKey(0)(_ + _)也可以写成rdd.aggregateByKey(0)(_ + _, _ + _)
    val foldRDD: RDD[(Int, Int)] = rdd.foldByKey(0)(_ + _)
    val aggRDD: RDD[(Int, Int)] = rdd.aggregateByKey(0)(_ + _, _ + _)
    val combineRDD: RDD[(Int, Int)] = rdd.combineByKey(x => x, (x: Int, y: Int) => (x + y), (x: Int, y: Int) => (x + y))
    combineRDD.collect().foreach(println)
  }
}


