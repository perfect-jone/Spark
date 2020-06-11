package com.atjone.bigdata.spark.SparkCore.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext}

object Spark20_groupByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("one", "two", "two", "three", "three", "three"))

    //"one" ==> ("one",1)
    val kvRDD: RDD[(String, Int)] = listRDD.map((_, 1))

    //groupByKey算子(k-v):按key分组,直接进行shuffle
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = kvRDD.groupByKey()

    //求和
    val wordCountRDD: RDD[(String, Int)] = groupByKeyRDD.map(x => (x._1, x._2.sum))

    //排序 x => x._1 可以简写为_._1,表示根据对偶元组的第一个数进行排序
    val sortDescRDD: RDD[(String, Int)] = wordCountRDD.sortBy(_._1, ascending = true)

    sortDescRDD.collect().foreach(println)
  }
}


