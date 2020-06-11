package com.atjone.bigdata.spark.SparkCore.key_value

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark27_hbase {
  def main(args: Array[String]): Unit = {

    //创建SparkContext对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("")
    val sc: SparkContext = new SparkContext(sparkConf)

    //构建HBase配置信息
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103")
    conf.set(TableInputFormat.INPUT_TABLE, "student")

    //从HBase读取数据形成RDD
    /**
      * @param fClass Class of the InputFormat
      * @param kClass Class of the keys
      * @param vClass Class of the values
      */

      //SparkContext对象会有一个专门的方法来访问其他跟Hadoop相关的框架，比如HBase，Hive
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    hbaseRDD.foreach {
      case (rowkey, result) => {
        //rawCells返回这一行的所有单元格
        val cells: Array[Cell] = result.rawCells()
        for (cell <- cells) {
          val rowkey: String = Bytes.toString(CellUtil.cloneRow(cell))
          val value: String = Bytes.toString(CellUtil.cloneValue(cell))
          val family: String = Bytes.toString(CellUtil.cloneQualifier(cell))
          val qualifier: String = Bytes.toString(CellUtil.cloneFamily(cell))
          println("rowkey:" + rowkey + "\tfamily:qualifier " + family + ":" + qualifier + "\tvalue:" + value)
        }
      }
    }


    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1006", "pating"), ("1007", "jone"), ("1008", "xiaopangpan")))
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowkey, value) => {
        val put = new Put(Bytes.toBytes(rowkey))
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(value))
        (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
      }
    }
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
    putRDD.saveAsHadoopDataset(jobConf)

    //释放资源
    sc.stop()
  }
}


