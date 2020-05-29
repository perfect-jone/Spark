package com.atjone.bigdata.spark.operate.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_reduceByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(List("one", "two", "two", "three", "three", "three"))

    //"one" ==> ("one",1)
    val kvRDD: RDD[(String, Int)] = listRDD.map((_, 1))

    //reduceByKey算子(k-v):将相同key的值聚合到一起
    //(x,y)=>(x+y)
    val reduceByKeyRDD: RDD[(String, Int)] = kvRDD.reduceByKey(_ + _)

    //排序 x => x._2 可以简写为_._2，表示根据对偶元组的第二个数进行排序，如需要降序排序，可将ascending=false
    val sortDescRDD: RDD[(String, Int)] = reduceByKeyRDD.sortBy(_._2, ascending = true)

    sortDescRDD.collect().foreach(println)
  }
}


