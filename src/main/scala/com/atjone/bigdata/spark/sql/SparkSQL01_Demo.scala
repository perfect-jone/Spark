package com.atjone.bigdata.spark.sql

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {

    //SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    //SparkContext

    //SparkSession
    //val spark: SparkSession = new SparkSession(sparkConf)
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val dataFrame: DataFrame = spark.read.json("in/user.json")
    dataFrame.show()

    //释放资源
    spark.stop()

    //SparkSQL
  }
}
