package com.atjone.bigdata.spark.SparkCore.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_repartition {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("repartitionRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.parallelize(1 to 16, 4)

    // repartition算子:根据分区数，重新通过网络随机洗牌所有数据。
    val repartitionRDD: RDD[Int] = listRDD.repartition(2)
    repartitionRDD.glom.collect.foreach(arr => println(arr.mkString(",")))

    /**
      * coalesce和repartition的区别：
      * 1.coalesce进行重分区，可以选择是否进行shuffle过程，由shuffle:Boolean=ture/false决定
      * 2.repartition的底层代码调用的是coalesce(numPartitions,shuffle=true)
      */

  }
}
