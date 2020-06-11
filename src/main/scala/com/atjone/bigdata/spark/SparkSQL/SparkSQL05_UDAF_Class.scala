package com.atjone.bigdata.spark.SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql._

object SparkSQL05_UDAF_Class {
  def main(args: Array[String]): Unit = {

    // SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    // SparkSession中包含SparkContext

    // SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //进入转换之前，需要引入隐式转换规则，是SparkSession对象的名字

    val rdd = spark.sparkContext.makeRDD(List(("zhangsan",21),("lisi",31),("wangwu",41)))
    val df: DataFrame = rdd.toDF("name","age")
    val userDS: Dataset[UserBean] = df.as[UserBean]

    // 创建聚合函数对象
    val udacf = new MyAgeAvgClassFunction

    // 将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udacf.toColumn.name("avgAge")

    // 应用函数
    userDS.select(avgCol).show

    // 释放资源
    spark.stop()
  }
}

/**
  * 用户自定义聚合函数(强类型：就是在有结构的基础上，再给数据加入类型):
  * 1.继承Aggregator
  * 2.给Aggregator设定泛型[-IN, BUF, OUT]
  * 3.实现方法
  */
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {

  // 初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  // 聚合数据
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  // 将多个节点的缓冲区合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  // 计算结果
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

case class UserBean(name: String, var age: Int)

case class AvgBuffer(var sum: Int, var count: Int)
