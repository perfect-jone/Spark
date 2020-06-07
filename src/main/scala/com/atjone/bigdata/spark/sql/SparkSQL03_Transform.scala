package com.atjone.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL03_Transform {
  def main(args: Array[String]): Unit = {

    //SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")

    //SparkSession中包含SparkContext

    //SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._ //进入转换之前，需要引入隐式转换规则，是SparkSession对象的名字
    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD((List((1, "zhangsan", 20), (2, "lisi", 30), (3, "wangwu", 40))))

    //转换为DataFrame
    val dataFrame: DataFrame = rdd.toDF("id", "name", "age")

    //转化为DataSet
    val dataSet: Dataset[User] = dataFrame.as[User]

    //转化为DataFrame
    val dataFrame2: DataFrame = dataSet.toDF()

    //转化为RDD
    val rdd2: RDD[Row] = dataFrame2.rdd
/*    rdd2.foreach {
      //获取数据时，可以通过索引访问
      row => println(row.getInt(0) + "," + row.getString(1) + "," + row.getInt(2))
    }*/

    //RDD转换为DataSet
    val userRDD: RDD[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }
    val userDataSet: Dataset[User] = userRDD.toDS()
    //DataSet转换为RDD
    val userRDD2: RDD[User] = userDataSet.rdd
    userRDD2.foreach(println)
    //释放资源
    spark.stop()

  }
}

case class User(id: Int, name: String, age: Int)
