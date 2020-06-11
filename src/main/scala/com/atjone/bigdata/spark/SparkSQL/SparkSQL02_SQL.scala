package com.atjone.bigdata.spark.SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL02_SQL {
  def main(args: Array[String]): Unit = {

    //SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    //SparkContext

    //SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataFrame: DataFrame = spark.read.json("in/user.json")

    //SparkSQL
    dataFrame.createOrReplaceTempView("user")
    spark.sql("select name,age from user where age >= 30").show()

    //释放资源
    spark.stop()


  }
}
