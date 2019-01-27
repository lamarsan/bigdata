package com.spark.streaming

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp {
  def main(args : Array[String]) : Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //如果使用了stateful的算子，必须要设置checkpoint
    //在生产环境中，建议把checkpoint设置到HDFS的某个文件夹中
    //ssc.checkpoint(".")
    val lines = ssc.socketTextStream("192.168.110.128",6789)

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    //将结果写入到MySQL
    result.foreachRDD(rdd =>{
      rdd.foreachPartition(partitionOfRecords =>  {
        val connection = createConnection()
        partitionOfRecords.foreach(record => {
          val sql = "insert into wordcount(word, wordcount) values('" + record._1 + "'," + record._2 + ")"

          connection.createStatement().execute(sql)
        })

        connection.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取Mysql连接
    */
  def createConnection() = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/sparkstreaming?serverTimezone=GMT%2B8&&useCursorFetch=true&&characterEncoding=utf8","root","root")
  }
}
