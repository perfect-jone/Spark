package com.atjone.bigdata.spark.operate.value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_groupBy {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByRDD")
    val sc: SparkContext = new SparkContext(conf)

    //从内存中创建对象
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

    //groupBy算子:生成数据，按照指定的规则进行分组
    //Int表示x % 2，Iterable[Int]表示把List(1, 2, 3, 4)中的每一个数据按照x % 2进行分组
    //(0,CompactBuffer(2, 4))
    //(1,CompactBuffer(1, 3))
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(x => x % 2)
    groupByRDD.collect().foreach(println)
  }
}
