package com.atjone.bigdata.spark.SparkCore.key_value

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object Spark26_mysql {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    //定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop101:3306/rdd?useUnicode=true&characterEncoding=UTF-8"
    val userName = "root"
    val passWord = "123456"

    //查询数据库
    //sql语句
    /*    val sql = "select name,age from user where id >= ? and id <= ?"
        val jdbcRDD = new JdbcRDD(
          sc,
          () => {
            //获取数据库连接对象
            Class.forName(driver)
            DriverManager.getConnection(url, userName, passWord)
          },
          sql,
          1,
          3,
          2,
          (rs) => {
            println(rs.getString(1) + "," + rs.getInt(2))
          }
        )
        jdbcRDD.collect()*/


    //保存数据
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("小呆", 27), ("小潘", 25), ("小小潘", 18)))
    rdd.foreachPartition(datas => {
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWord)
      datas.foreach {
        case (name, age) => {
          try {
            val sql = "insert into user(name,age) values(?,?)"
            val statement: PreparedStatement = connection.prepareStatement(sql)
            statement.setString(1, name)
            statement.setInt(2, age)
            statement.executeUpdate()
            statement.close()
          } catch {
            case e:Throwable => e.printStackTrace()
          }
        }
      }
      connection.close()
    })
    //释放资源
    sc.stop()
  }
}


