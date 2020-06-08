package com.atjone.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL04_UDAF {
  def main(args: Array[String]): Unit = {

    //SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    //SparkSession中包含SparkContext

    //SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //进入转换之前，需要引入隐式转换规则，是SparkSession对象的名字

    val rdd = spark.sparkContext.makeRDD(List(("zhangsan",21),("lisi",31),("wangwu",41)))
    val df: DataFrame = rdd.toDF("name","age")
    df.createTempView("user")

    //创建用户自定义聚合函数
    val udaf = new MyAgeAvgFunction

    //注册用户自定义聚合函数
    spark.udf.register("avgAge",udaf)

    //使用用户自定义聚合函数

    spark.sql("select avgAge(age) from user").show()
    //释放资源
    spark.stop()
  }
}

/**
  * 用户自定义聚合函数:
  * 1.继承UserDefinedAggreagteFunction
  * 2.实现方法
  */
class MyAgeAvgFunction extends UserDefinedAggregateFunction {

  // 函数输入的数据结构
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  // 计算时的缓冲区的数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  // 函数返回的数据类型
  override def dataType: DataType = DoubleType

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算前缓冲区的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  // 更新数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // 将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
