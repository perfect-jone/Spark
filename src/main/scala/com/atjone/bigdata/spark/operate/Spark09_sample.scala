package com.atjone.bigdata.spark.operate

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_sample {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sampleRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    // sample算子:以指定的随机种子随机抽样出数量为fraction的数据，
    // withReplacement表示是抽出的数据是否放回，true为有放回的抽样，false为无放回的抽样，
    // seed用于指定随机数生成器种子。
    val sampleRDD: RDD[Int] = listRDD.sample(false, 0.3, 1)
    sampleRDD.collect().foreach(println)
  }
}
