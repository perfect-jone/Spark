package com.atjone.bigdata.spark.operate.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_aggregateByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)

    /**
      * aggregateByKey[U: ClassTag](zeroValue: U)(seqOp: (U, V) => U,combOp: (U, U) => U): RDD[(K, U)]
      * 参数描述：
      * （1）zeroValue：给每一个分区中的每一个key对应的value一个初始值；
      * （2）seqOp：函数用于在每一个分区中用初始值逐步迭代value；
      * （3）combOp：函数用于合并每个分区中的结果。
      */

    // ((x, y) => math.max(x, y), (x, y) => x + y) 可以简写为 (math.max(_,_),_+_)
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    aggRDD.collect().foreach(println)
  }
}


